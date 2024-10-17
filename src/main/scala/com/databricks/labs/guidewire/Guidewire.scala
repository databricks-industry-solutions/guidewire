package com.databricks.labs.guidewire

import com.amazonaws.services.s3.AmazonS3URI
import io.delta.standalone.actions.{Action, Metadata}
import io.delta.standalone.{DeltaLog, Operation}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{SaveMode, SparkSession, functions}
import org.apache.spark.util.SerializableConfiguration
import org.slf4j.{Logger, LoggerFactory}

import java.util
import scala.collection.JavaConverters._

object Guidewire extends Serializable {

  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  private val checkpointsTable = "_checkpoints"
  private val configDeltaAbsolutePath = "io.delta.vacuum.relativize.ignoreError"

  /**
   * Entry point for guidewire connector
   *
   * @param manifestS3Uri S3 location were guidewire manifest can be found
   * @param databasePath  output location of delta table. Each tableName will be a child of that path
   * @param saveMode      whether we want to "Overwrite" or "Append" (by default)
   */
  def index(
             manifestS3Uri: String,
             databasePath: String,
             saveMode: SaveMode = SaveMode.Append,
             enforceGuidewireTimestamp: Boolean = true
           ): Unit = {

    // If Overwrite, we do not care about checkpoints as we will be reindexing the whole table
    // For Append, we first load last processed timestamp for each table
    // Checkpoints are stored as json under databasePath/_checkpoints
    val checkpoints: Map[String, Long] = saveMode match {
      case SaveMode.Append => Guidewire.loadCheckpoints(databasePath)
      case SaveMode.Overwrite => Map.empty[String, Long]
      case _ => throw new IllegalArgumentException("Only [Append] or [Overwrite] are supported")
    }

    // We access guidewire manifest
    val manifest = Guidewire.readManifest(manifestS3Uri)

    // Given the list of available files (manifest) and optional checkpoints, we process each table in parallel
    val batches = Guidewire.processManifest(manifest, checkpoints, databasePath, enforceGuidewireTimestamp)

    // Upon success, we save new checkpoints for future index
    Guidewire.saveCheckpoints(batches, databasePath, saveMode)
  }

  private[guidewire] def readManifest(manifestLocation: String): Map[String, ManifestEntry] = {
    logger.info("Reading manifest file")
    val s3 = S3Access.build
    val manifestUri = new AmazonS3URI(manifestLocation)
    val manifest = GuidewireUtils.readManifest(s3.readString(manifestUri.getBucket, manifestUri.getKey))
    logger.info(s"Found ${manifest.size} table(s) to process")
    manifest
  }

  private[guidewire] def processManifest(
                                          manifest: Map[String, ManifestEntry],
                                          checkpoints: Map[String, Long] = Map.empty[String, Long],
                                          databasePath: String,
                                          enforceGuidewireTimestamp: Boolean = true
                                        ): Map[String, List[BatchResult]] = {

    logger.info(s"Distributing ${manifest.size} table(s) against multiple executor(s)")
    val manifestRdd = SparkSession.active.sparkContext.makeRDD(manifest.toList).repartition(manifest.size)

    manifestRdd.cache() // materialize partitioning
    manifestRdd.count()

    // User may have set system properties for proxy
    // Let's carried those across executors
    val systemPropertiesB = SparkSession.active.sparkContext.broadcast(System.getProperties)
    val enforceGuidewireTimestampB = SparkSession.active.sparkContext.broadcast(enforceGuidewireTimestamp)

    // Serialize some configuration to be used at executor level
    val hadoopConfiguration = SparkSession.active.sparkContext.hadoopConfiguration
    val hadoopConfigurationS = new SerializableConfiguration(hadoopConfiguration)
    val hadoopConfigurationB = SparkSession.active.sparkContext.broadcast(hadoopConfigurationS)

    val databasePathB = SparkSession.active.sparkContext.broadcast(databasePath)
    val checkpointsB = if (checkpoints.nonEmpty) {
      logger.info("Processing guidewire as data increment")
      SparkSession.active.sparkContext.broadcast(checkpoints)
    } else {
      logger.info("Reindexing all guidewire database")
      SparkSession.active.sparkContext.broadcast(Map.empty[String, Long])
    }

    // Distributed process, each executor will handle a given table
    manifestRdd.map({ case (tableName, manifestEntry) =>

      // Deserialize hadoop configuration
      val hadoopConfiguration = hadoopConfigurationB.value.value

      // Deserialize System properties
      val systemProperties = systemPropertiesB.value
      System.setProperties(systemProperties)

      // Acting as a shallow clone, we do not want framework to relativize paths
      hadoopConfiguration.setBoolean(configDeltaAbsolutePath, true)

      // Retrieve last checkpoints
      val lastProcessedTimestamp = checkpointsB.value.getOrElse(tableName, -1L)

      // EDGE CASE#1: Sometimes Guidewire does not seem to enforce last updated timestamp in manifest
      // This results in folders being skipped. We might want to relax this constraint on an exception basis by
      // providing framework with an optional parameter
      val lastSuccessfulWriteTimestamp = if (enforceGuidewireTimestampB.value) {
        manifestEntry.lastSuccessfulWriteTimestamp.toLong
      } else {
        Long.MaxValue
      }

      // Ensure task serialization - this happens at executor level
      val s3 = S3Access.build

      // Process each schema directory, starting from eldest to most recent
      val dataFilesUri = new AmazonS3URI(manifestEntry.getDataFilesPath)
      val schemaHistory = manifestEntry.schemaHistory.toList.sortBy(_._2.toLong).zipWithIndex
      val batches = schemaHistory.flatMap({ case ((schemaId, _), i) =>

          // For each schema directory, find associated timestamp folders to process
          val schemaDirectory = s"${dataFilesUri.getKey}/$schemaId/"
          val schemaTimestamps = s3.listTimestampDirectories(dataFilesUri.getBucket, schemaDirectory)

          // We only want to process recent information (since last checkpoint or 0 if overwrite)
          // Equally, we want to ensure folder was entirely committed and therefore defined in manifest
          val schemaCommittedTimestamps = schemaTimestamps
            .sorted
            .zipWithIndex
            .filter(_._1 <= lastSuccessfulWriteTimestamp) // Ensure directory we find was committed to manifest
            .filter(_._1 > lastProcessedTimestamp) // Ensure directory was not yet processed

          // Get files for each timestamp folder
          schemaCommittedTimestamps.map({ case (committedTimestamp, j) =>

            // List all parquet files
            val timestampDirectory = s"${dataFilesUri.getKey}/$schemaId/$committedTimestamp/"
            val timestampFiles = s3.listParquetFiles(dataFilesUri.getBucket, timestampDirectory).filter(_.getSize > 0L)

            // Building a new batch of files resulting in a new delta version
            val batch = if (timestampFiles.length == 0) {
              logger.error("Could not find any file in directory [{}]", timestampDirectory)
              None: Option[Batch]
            } else {

              // For the first committed timestamp folder in a given schema, extract schema from a sample parquet file
              // This assumes schema consistency within guidewire (same schema over different subfolder timestamps)
              val metadata = if (j == 0) {

                // For convenience, let's read the smallest file available
                // EDGE CASE#2: Unfortunately, some files are either empty or non null but without any record
                // We'll recursively read files from smallest to largest until schema can be derived
                logger.debug("Reading schema for [{}] (new fingerprint)", timestampDirectory)
                val sampleFiles = timestampFiles.sortBy(_.getSize)
                GuidewireUtils.readSchemaFromFiles(s3, dataFilesUri.getBucket, sampleFiles)
              } else {
                // This is not the first committed folder, no need to define schema
                logger.debug("Skipping schema for [{}] (same fingerprint)", timestampDirectory)
                None: Option[Metadata]
              }

              // Wrap all commit information into a case class and keep track of the order that was processed
              Some(Batch(schemaId, committedTimestamp, timestampFiles, metadata))
            }

            (batch, (i, j))
          })
        })
        // Ensure we had files to constitute a batch
        .filter(_._1.isDefined)
        // Sort all batches (schema first, then timestamp)
        .sortBy({ case (_, (schemaId, commitId)) => (schemaId, commitId) })
        // Get individual batch
        .map(_._1.get)

      if (lastProcessedTimestamp > 0) {
        // We have checkpoints, this is an append function
        saveDeltaLogAppend(hadoopConfiguration, tableName, batches, databasePathB.value)
      } else {
        // We do not have checkpoint for this table yet or defined SaveMode.Overwrite
        saveDeltaLogOverwrite(hadoopConfiguration, tableName, batches, databasePathB.value)
      }

      // Keep track of processed timestamps so that we can save checkpoints
      (tableName, batches.map(b => BatchResult(b.schemaId, b.commitTimestamp, b.filesToAdd.size)))

    }).collect().toMap

  }

  private[guidewire] def saveDeltaLogOverwrite(
                                                hadoopConfiguration: Configuration,
                                                tableName: String,
                                                batches: List[Batch],
                                                databasePath: String
                                              ): Unit = {

    val tablePath = new Path(databasePath + Path.SEPARATOR + tableName)
    val fs = FileSystem.get(hadoopConfiguration)
    if (fs.exists(tablePath)) fs.delete(tablePath, true)
    saveDeltaLogAppend(hadoopConfiguration, tableName, batches, databasePath)
  }

  private[guidewire] def saveDeltaLogAppend(
                                             hadoopConfiguration: Configuration,
                                             tableName: String,
                                             batches: List[Batch],
                                             databasePath: String
                                           ): Unit = {

    val tablePath = databasePath + Path.SEPARATOR + tableName
    batches.map(batch => {
      val log = DeltaLog.forTable(hadoopConfiguration, tablePath)
      val version = batch.metadata match {
        case Some(metadata) =>
          // Schema has changed, we overwrite table with new files and new schema
          val filesToRemove = log.snapshot().getAllFiles.asScala.map(_.remove()).asJava
          val filesToAdd = batch.filesToAdd.asJava
          val actions = new util.ArrayList[Action]()
          actions.addAll(filesToRemove)
          actions.addAll(filesToAdd)
          val tx = log.startTransaction()
          tx.updateMetadata(metadata)
          val operation = if (log.tableExists()) {
            new Operation(Operation.Name.UPGRADE_SCHEMA)
          } else {
            new Operation(Operation.Name.CREATE_TABLE)
          }
          tx.commit(actions, operation, "guidewire")
          tx.readVersion()
        case None =>
          val tx = log.startTransaction()
          val operation = new Operation(Operation.Name.WRITE)
          tx.commit(batch.filesToAdd.asJava, operation, "guidewire")
          tx.readVersion()
      }
      (batch, version)
    })
  }

  private[guidewire] def saveCheckpoints(
                                          batches: Map[String, List[BatchResult]],
                                          databasePath: String,
                                          saveMode: SaveMode
                                        ): Unit = {
    logger.info("Saving checkpoints to delta")
    val spark = SparkSession.active
    import spark.implicits._
    val checkpoints = batches.flatMap({ case (tableName, tableBatches) =>
      tableBatches.map(tableBatch => {
        (tableName, tableBatch.schemaId, tableBatch.commitTimestamp, tableBatch.numFiles)
      })
    }).toList.toDF("tableName", "processedSchema", "processedTimestamp", "processedFiles")
    checkpoints.write.format("delta").mode(saveMode).save(s"$databasePath/$checkpointsTable")
  }

  private[guidewire] def loadCheckpoints(databasePath: String): Map[String, Long] = {
    val fs = FileSystem.get(SparkSession.active.sparkContext.hadoopConfiguration)
    if (fs.exists(new Path(s"$databasePath/$checkpointsTable"))) {
      logger.info("Loading checkpoints from delta")
      val batchesDF = SparkSession.active.read.format("delta").load(s"$databasePath/$checkpointsTable")
      batchesDF
        .groupBy("tableName")
        .agg(functions.max("processedTimestamp").alias("processedTimestamp"))
        .rdd
        .map(r => {
          (r.getAs[String]("tableName"), r.getAs[Long]("processedTimestamp"))
        })
        .collect()
        .toMap
    } else {
      logger.warn("No previous checkpoints found")
      Map.empty[String, Long]
    }
  }

}
