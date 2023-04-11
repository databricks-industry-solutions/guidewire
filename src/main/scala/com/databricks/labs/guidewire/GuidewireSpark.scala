package com.databricks.labs.guidewire

import com.amazonaws.auth.AWSCredentials
import com.amazonaws.services.s3.AmazonS3URI
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

import java.nio.charset.Charset
import scala.annotation.tailrec

class GuidewireSpark(region: Option[String] = None, credential: Option[AWSCredentials] = None) extends Serializable {

  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def readManifest(manifestLocation: String): Map[String, ManifestEntry] = {
    logger.info("Reading manifest file")
    val s3 = new S3Access(region, credential)
    val manifestUri = new AmazonS3URI(manifestLocation)
    GuidewireUtils.readManifest(s3.readString(manifestUri.getBucket, manifestUri.getKey))
  }

  def readBatches(manifest: Map[String, ManifestEntry]): Map[String, List[GwBatch]] = {

    logger.info(s"Distributing ${manifest.size} table(s) against multiple executor(s)")
    val manifestRdd = SparkSession.active.sparkContext.makeRDD(manifest.toList).repartition(manifest.size)
    manifestRdd.cache()

    // Distributed process, each executor will handle a given table
    manifestRdd.mapValues(manifestEntry => {

      // Ensure task serialization - this happens at executor level
      val s3 = new S3Access(region, credential)

      // Process each schema directory
      val dataFilesUri = new AmazonS3URI(manifestEntry.getDataFilesPath)
      val schemaHistory = manifestEntry.schemaHistory.toList.sortBy(_._2.toLong).zipWithIndex
      val batches = schemaHistory.flatMap({ case ((schemaId, lastUpdatedTs), i) =>

        // For each schema directory, find timestamp folders to process
        val schemaDirectory = s"${dataFilesUri.getKey}/$schemaId/"
        val schemaTimestamps = s3.listTimestampDirectories(dataFilesUri.getBucket, schemaDirectory).sorted
        val schemaCommittedTimestamps = schemaTimestamps.filter(_ <= lastUpdatedTs.toLong)

        // Get files for each timestamp folder
        schemaCommittedTimestamps.zipWithIndex.map({ case (committedTimestamp, j) =>
          val timestampDirectory = s"${dataFilesUri.getKey}/$schemaId/$committedTimestamp/"
          val timestampFiles = s3.listParquetFiles(dataFilesUri.getBucket, timestampDirectory)

          // For the first committed timestamp folder, extract schema from parquet file
          val gwSchema = if (j == 0) {
            val sampleFile = timestampFiles.minBy(_.size)
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

      // Every time schema changes, we overwrite data (we assume old data is ported out)
      accumulateFiles(batches)

    }).collect().toMap

  }

  def saveDeltaLog(batches: Map[String, List[GwBatch]], databasePath: String): Unit = {
    // Serialize all batches as delta logs
    batches.foreach({ case (tableName, batches) =>
      // Delta log will be stored on a given path accessible by Spark on Databricks
      val fs = FileSystem.get(SparkSession.active.sparkContext.hadoopConfiguration)
      val tablePath = new Path(databasePath, tableName)
      val deltaPath = new Path(tablePath, "_delta_log")
      if (fs.exists(deltaPath)) fs.delete(deltaPath, true)
      fs.mkdirs(deltaPath)
      batches.foreach(batch => {
        val deltaFile = new Path(deltaPath, GuidewireUtils.generateFileName(batch.version))
        val fos = fs.create(deltaFile)
        fos.write(batch.toJson.getBytes(Charset.defaultCharset()))
        fos.close()
      })
    })
  }

  @tailrec
  private def accumulateFiles(
                               batches: List[GwBatch],
                               filesToRemove: Array[GwFile] = Array.empty[GwFile],
                               processed: List[GwBatch] = List.empty[GwBatch]
                             ): List[GwBatch] = {

    if (batches.isEmpty) return processed
    val newBatch = batches.head
    if (newBatch.schema.isDefined) {
      // change of schema, add previous files to remove
      accumulateFiles(batches.tail, newBatch.filesToAdd, processed :+ newBatch.copy(filesToRemove = filesToRemove))
    } else {
      // schema did not change, keep accumulating files
      accumulateFiles(batches.tail, filesToRemove ++ newBatch.filesToAdd, processed :+ newBatch)
    }
  }

}
