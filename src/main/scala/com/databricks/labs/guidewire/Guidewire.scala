package com.databricks.labs.guidewire

import com.amazonaws.services.s3.AmazonS3URI
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{SaveMode, SparkSession, functions}
import org.slf4j.{Logger, LoggerFactory}

import java.nio.charset.Charset

object Guidewire extends Serializable {

  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val checkpointsTable = "_checkpoints"
  val deltaManifest = "_delta_log"

  def index(
             manifestS3Uri: String,
             databasePath: String,
             saveMode: SaveMode = SaveMode.Append
           ): Unit = {
    val manifest = Guidewire.readManifest(manifestS3Uri)
    val checkpoints: Map[String, Long] = saveMode match {
      case SaveMode.Append => Guidewire.loadCheckpoints(databasePath)
      case SaveMode.Overwrite => Map.empty[String, Long]
      case _ => throw new IllegalArgumentException("Unsupported Save Mode")
    }
    val batches = Guidewire.processManifest(manifest, checkpoints)
    Guidewire.saveDeltaLog(batches, databasePath, saveMode)
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
                                          checkpoints: Map[String, Long] = Map.empty[String, Long]
                                        ): Map[String, List[GwBatch]] = {

    logger.info(s"Distributing ${manifest.size} table(s) against multiple executor(s)")
    val manifestRdd = SparkSession.active.sparkContext.makeRDD(manifest.toList).repartition(manifest.size)

    manifestRdd.cache()
    manifestRdd.count()

    val checkpointsB = if (checkpoints.nonEmpty) {
      logger.info("Processing guidewire as data increment")
      SparkSession.active.sparkContext.broadcast(checkpoints)
    } else {
      logger.info("Reindexing all guidewire database")
      SparkSession.active.sparkContext.broadcast(Map.empty[String, Long])
    }

    // Distributed process, each executor will handle a given table
    val batchRdd = manifestRdd.map({ case (tableName, manifestEntry) =>

      // Retrieve last checkpoints
      val lastProcessedTimestamp = checkpointsB.value.getOrElse(tableName, -1L)

      // Ensure task serialization - this happens at executor level
      val s3 = S3Access.build

      // Process each schema directory, sorting from eldest to youngest
      val dataFilesUri = new AmazonS3URI(manifestEntry.getDataFilesPath)
      val schemaHistory = manifestEntry.schemaHistory.toList.sortBy(_._2.toLong).zipWithIndex
      val batches = schemaHistory.flatMap({ case ((schemaId, lastUpdatedTs), i) =>

        // For each schema directory, find associated timestamp folders to process
        val schemaDirectory = s"${dataFilesUri.getKey}/$schemaId/"
        val schemaTimestamps = s3.listTimestampDirectories(dataFilesUri.getBucket, schemaDirectory).sorted
        val schemaCommittedTimestamps = schemaTimestamps
          .filter(_ <= lastUpdatedTs.toLong) // Ensure directory we find was committed to manifest
          .filter(_ > lastProcessedTimestamp) // Ensure directory was not yet processed

        // Get files for each timestamp folder
        schemaCommittedTimestamps.zipWithIndex.map({ case (committedTimestamp, j) =>
          val timestampDirectory = s"${dataFilesUri.getKey}/$schemaId/$committedTimestamp/"
          val timestampFiles = s3.listParquetFiles(dataFilesUri.getBucket, timestampDirectory)

          // For the first committed timestamp folder, extract schema from parquet file
          // This assumes schema consistency within guidewire (same schema over different subfolder timestamps)
          val gwSchema = if (j == 0) {
            // For convenience, let's read the smallest file available
            val sampleFile = timestampFiles.minBy(_.size)
            // And return associated spark schema
            val sampleSchema = GuidewireUtils.readSchema(s3.readByteArray(dataFilesUri.getBucket, sampleFile.getKey))
            Some(GwSchema(sampleSchema, committedTimestamp))
          } else None: Option[GwSchema]
          val gwBatch = GwBatch(committedTimestamp, filesToAdd = timestampFiles, schema = gwSchema)
          (gwBatch, (i, j))
        })
      }).sortBy({ case (_, (schemaId, batchId)) =>
        // Sort all batches (schema first, then timestamp)
        (schemaId, batchId)
      }).map(_._1).zipWithIndex.map({ case (batch, batchId) =>
        // And get Batch version Id used for delta log
        batch.copy(version = batchId)
      })

      (tableName, batches)

    })

    // We got a map as input, let's collect back as a map
    // We distributed this process as guidewire manifest may contain lots of table
    // But the resulting process is a collection that fits well in memory
    batchRdd.collect().toMap
  }

  private[guidewire] def saveCheckpoints(
                                          batches: Map[String, List[GwBatch]],
                                          databasePath: String,
                                          saveMode: SaveMode
                                        ): Unit = {
    logger.info("Saving checkpoints to delta")
    val spark = SparkSession.active
    import spark.implicits._
    val checkpoints = batches.filter(_._2.nonEmpty).map({ case (tableName, tableBatches) =>
      (tableName, tableBatches.maxBy(_.timestamp).timestamp, System.currentTimeMillis())
    }).toList.toDF("tableName", "lastProcessed", "executionTimestamp")
    checkpoints.write.format("json").mode(saveMode).save(s"$databasePath/$checkpointsTable")
  }

  private[guidewire] def loadCheckpoints(
                                          databasePath: String
                                        ): Map[String, Long] = {
    val fs = FileSystem.get(SparkSession.active.sparkContext.hadoopConfiguration)
    if (fs.exists(new Path(s"$databasePath/$checkpointsTable"))) {
      logger.info("Loading checkpoints from delta")
      val batchesDF = SparkSession.active.read.format("json").load(s"$databasePath/$checkpointsTable")
      batchesDF.groupBy("tableName").agg(functions.max("lastProcessed").alias("lastProcessed")).rdd.map(r => {
        (r.getAs[String]("tableName"), r.getAs[Long]("lastProcessed"))
      }).collect().toMap
    } else {
      logger.warn("No previous checkpoints found")
      Map.empty[String, Long]
    }
  }

  private[guidewire] def saveDeltaLog(batches: Map[String, List[GwBatch]], databasePath: String, saveMode: SaveMode): Unit = {
    batches.foreach({ case (tableName, tableBatches) =>
      logger.info(s"Saving guidewire delta logs for table [$tableName]")
      saveMode match {
        case SaveMode.Overwrite => saveDeltaLogOverwrite(tableName, tableBatches, databasePath)
        case SaveMode.Append => saveDeltaLogAppend(tableName, tableBatches, databasePath)
        case _ => throw new IllegalArgumentException("Only [Append] or [Overwrite] are supported")
      }
    })
  }

  private[guidewire] def saveDeltaLogAppend(tableName: String, batches: List[GwBatch], databasePath: String): Unit = {
    val fs = FileSystem.get(SparkSession.active.sparkContext.hadoopConfiguration)
    val tablePath = new Path(databasePath, tableName)
    val deltaPath = new Path(tablePath, deltaManifest)
    if (!fs.exists(deltaPath)) {
      saveDeltaLogOverwrite(tableName, batches, databasePath)
    } else {
      val deltaFiles = fs.listStatus(deltaPath)
      val previousBatches = deltaFiles.map(deltaFile => GuidewireUtils.getBatchFromDeltaLog(deltaFile.getPath)).toList
      val lastVersion = previousBatches.map(_.version).max
      val updatedBatches = previousBatches ++ batches.map(b => b.copy(version = b.version + lastVersion + 1))
      val accumulatedBatches = GuidewireUtils.unregisterFilesPropagation(updatedBatches.sortBy(_.version))
      accumulatedBatches.filter(_.version > lastVersion).foreach(batch => {
        val deltaFile = new Path(deltaPath, GuidewireUtils.generateFileName(batch.version))
        val fos = fs.create(deltaFile)
        fos.write(batch.toJson.getBytes(Charset.defaultCharset()))
        fos.close()
      })
    }
  }

  private[guidewire] def saveDeltaLogOverwrite(tableName: String, batches: List[GwBatch], databasePath: String): Unit = {
    val fs = FileSystem.get(SparkSession.active.sparkContext.hadoopConfiguration)
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
