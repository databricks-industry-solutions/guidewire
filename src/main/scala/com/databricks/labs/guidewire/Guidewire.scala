package com.databricks.labs.guidewire

import com.amazonaws.services.s3.AmazonS3URI
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{SaveMode, SparkSession, functions}
import org.apache.spark.util.SerializableConfiguration
import org.slf4j.{Logger, LoggerFactory}

import java.nio.charset.Charset

object Guidewire extends Serializable {

  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val checkpointsTable = "_checkpoints"
  val deltaManifest = "_delta_log"

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
             saveMode: SaveMode = SaveMode.Append
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
    val batches = Guidewire.processManifest(manifest, checkpoints, databasePath)

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
                                          databasePath: String
                                        ): Map[String, List[BatchResult]] = {

    logger.info(s"Distributing ${manifest.size} table(s) against multiple executor(s)")
    val manifestRdd = SparkSession.active.sparkContext.makeRDD(manifest.toList).repartition(manifest.size)

    manifestRdd.cache() // materialize partitioning
    manifestRdd.count()

    // Serialize some configuration to be used at executor level
    val hadoopConfiguration = SparkSession.active.sparkContext.hadoopConfiguration
    val hadoopConfigurationS = new SerializableConfiguration(hadoopConfiguration)
    val hadoopConfigurationB = SparkSession.active.sparkContext.broadcast(hadoopConfigurationS)
    SparkSession.active.sparkContext.hadoopConfiguration


    val databasePathB = SparkSession.active.sparkContext.broadcast(databasePath)
    val checkpointsB = if (checkpoints.nonEmpty) {
      logger.info("Processing guidewire as data increment")
      SparkSession.active.sparkContext.broadcast(checkpoints)
    } else {
      logger.info("Reindexing all guidewire database")
      SparkSession.active.sparkContext.broadcast(Map.empty[String, Long])
    }

    // Distributed process, each executor will handle a given table
    val batchRdd = manifestRdd.map({ case (tableName, manifestEntry) =>

      // Deserialize hadoop configuration
      val hadoopConfiguration = hadoopConfigurationB.value.value

      // Retrieve last checkpoints
      val lastProcessedTimestamp = checkpointsB.value.getOrElse(tableName, -1L)

      // Ensure task serialization - this happens at executor level
      val s3 = S3Access.build

      // Process each schema directory, starting from eldest to most recent
      val dataFilesUri = new AmazonS3URI(manifestEntry.getDataFilesPath)
      val schemaHistory = manifestEntry.schemaHistory.toList.sortBy(_._2.toLong).zipWithIndex
      val batches = schemaHistory.flatMap({ case ((schemaId, lastUpdatedTs), i) =>

        // For each schema directory, find associated timestamp folders to process
        val schemaDirectory = s"${dataFilesUri.getKey}/$schemaId/"
        val schemaTimestamps = s3.listTimestampDirectories(dataFilesUri.getBucket, schemaDirectory)

        // We only want to process recent information (since last checkpoint or 0 if overwrite)
        // Equally, we want to ensure folder was entirely committed and therefore defined in manifest
        val schemaCommittedTimestamps = schemaTimestamps
          .sorted
          .zipWithIndex
          .filter(_._1 <= lastUpdatedTs.toLong)  // Ensure directory we find was committed to manifest
          .filter(_._1 > lastProcessedTimestamp) // Ensure directory was not yet processed

        // Get files for each timestamp folder
        schemaCommittedTimestamps.map({ case (committedTimestamp, j) =>

          // List all parquet files
          val timestampDirectory = s"${dataFilesUri.getKey}/$schemaId/$committedTimestamp/"
          val timestampFiles = s3.listParquetFiles(dataFilesUri.getBucket, timestampDirectory)

          // For the first committed timestamp folder in a given schema, extract schema from a sample parquet file
          // This assumes schema consistency within guidewire (same schema over different subfolder timestamps)
          val gwSchema = if (j == 0) {
            // For convenience, let's read the smallest file available that we will read in memory
            val sampleFile = timestampFiles.minBy(_.size)
            // And return associated spark schema, serialized as json as per delta requirement
            val sampleSchema = GuidewireUtils.readSchema(s3.readByteArray(dataFilesUri.getBucket, sampleFile.getKey))
            Some(GwSchema(sampleSchema, committedTimestamp))
          } else {
            // This is not the first committed folder, no need to define schema
            None: Option[GwSchema]
          }

          // Wrap all commit information into a case class and keep track of the order that was processed
          val gwBatch = GwBatch(committedTimestamp, schemaId = schemaId, filesToAdd = timestampFiles, schema = gwSchema)
          (gwBatch, (i, j))

        })

      }).sortBy({ case (_, (schemaId, commitId)) =>
        // Sort all batches (schema first, then timestamp)
        (schemaId, commitId)
      }).zipWithIndex.map({ case ((batch, _), commitId) =>
        // And get Batch version Id used for delta log
        batch.copy(version = commitId)
      })

      // Save our metadata to delta table
      if (lastProcessedTimestamp > 0) {
        // We processed data increment, appending to table
        saveDeltaLogAppend(hadoopConfiguration, tableName, batches, databasePathB.value)
      } else {
        // We either processed data fully or this is a new table we haven't seen yet, overwrite
        saveDeltaLogOverwrite(hadoopConfiguration, tableName, batches, databasePathB.value)
      }

      // Return list of processed batches that will be used for checkpointing
      val batchResults = batches.map(b => {
        BatchResult(schemaId = b.schemaId, commitTimestamp = b.timestamp, numFiles = b.filesToAdd.length)
      })
      (tableName, batchResults)

    })

    // We got a map as input, let's collect back as a map
    batchRdd.collect().toMap
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
        (tableName, tableBatch.schemaId, tableBatch.commitTimestamp, tableBatch.numFiles, System.currentTimeMillis())
      })
    }).toList.toDF("tableName", "processedSchema", "processedTimestamp", "processedFiles", "executionTimestamp")
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

  private[guidewire] def saveDeltaLogAppend(
                                             hadoopConfiguration: Configuration,
                                             tableName: String,
                                             batches: List[GwBatch],
                                             databasePath: String
                                           ): Unit = {
    val fs = FileSystem.get(hadoopConfiguration)
    val tablePath = new Path(databasePath, tableName)
    val deltaPath = new Path(tablePath, deltaManifest)
    if (!fs.exists(deltaPath)) {
      // Table does not exist, so equivalent of overwrite
      saveDeltaLogOverwrite(hadoopConfiguration, tableName, batches, databasePath)
    } else {
      // Here comes a nasty surprise...
      // As we append files, we need to keep track of previous versions in order to remove previous files
      val deltaFiles = fs.listStatus(deltaPath)
      val previousBatches = deltaFiles.map(deltaFile => GuidewireUtils.getBatchFromDeltaLog(deltaFile.getPath)).toList
      // Start this new increment with the correct version number
      val lastVersion = previousBatches.map(_.version).max
      val updatedBatches = previousBatches ++ batches.map(b => b.copy(version = b.version + lastVersion + 1))
      // Retrieve the list of all previously added files as they may need to be now marked as "remove" (edge conditions)
      val accumulatedBatches = GuidewireUtils.unregisterFilesPropagation(updatedBatches.sortBy(_.version))
      // Make sure we only persist the new batches, starting from last seen version
      accumulatedBatches.filter(_.version > lastVersion).foreach(batch => {
        val deltaFile = new Path(deltaPath, GuidewireUtils.generateFileName(batch.version))
        val fos = fs.create(deltaFile)
        fos.write(batch.toJson.getBytes(Charset.defaultCharset()))
        fos.close()
      })
    }
  }

  private[guidewire] def saveDeltaLogOverwrite(
                                                hadoopConfiguration: Configuration,
                                                tableName: String,
                                                batches: List[GwBatch],
                                                databasePath: String
                                              ): Unit = {
    val fs = FileSystem.get(hadoopConfiguration)
    val tablePath = new Path(databasePath, tableName)
    val deltaPath = new Path(tablePath, deltaManifest)
    if (fs.exists(deltaPath)) fs.delete(deltaPath, true)
    fs.mkdirs(deltaPath)
    // Every time schema changes, we need to ensure previous files are de-registered from delta log
    val accumulatedBatches = GuidewireUtils.unregisterFilesPropagation(batches)
    accumulatedBatches.foreach(batch => {
      val deltaFile = new Path(deltaPath, GuidewireUtils.generateFileName(batch.version))
      val fos = fs.create(deltaFile)
      fos.write(batch.toJson.getBytes(Charset.defaultCharset()))
      fos.close()
    })
  }

}
