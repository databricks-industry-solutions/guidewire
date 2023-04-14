package com.databricks.labs.guidewire

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.parquet.avro.AvroParquetReader
import org.apache.parquet.hadoop.ParquetReader
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.avro.SchemaConverters
import org.apache.spark.sql.types.StructType
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.read

import java.io.InputStream
import java.nio.charset.StandardCharsets
import scala.annotation.tailrec
import scala.util.Try

object GuidewireUtils {

  def generateFileName(version: Int): String = {
    require(version >= 0, "Version of batch must be greater or equal to zero")
    f"$version%020d.json"
  }

  @tailrec
  def unregisterFilesPropagation(
                                  batches: List[GwBatch],
                                  filesToRemove: Array[GwFile] = Array.empty[GwFile],
                                  processed: List[GwBatch] = List.empty[GwBatch]
                                ): List[GwBatch] = {

    if (batches.isEmpty) return processed
    val newBatch = batches.head
    if (newBatch.schema.isDefined) {
      // change of schema, add previous files to remove
      unregisterFilesPropagation(batches.tail, newBatch.filesToAdd, processed :+ newBatch.copy(filesToRemove = filesToRemove))
    } else {
      // schema did not change, keep accumulating files
      unregisterFilesPropagation(batches.tail, filesToRemove ++ newBatch.filesToAdd, processed :+ newBatch)
    }
  }

  def readManifest(manifestStream: InputStream): Map[String, ManifestEntry] = {
    val text: String = IOUtils.toString(manifestStream, StandardCharsets.UTF_8.name)
    readManifest(text)
  }

  def readManifest(manifestJson: String): Map[String, ManifestEntry] = {
    implicit val formats: DefaultFormats.type = DefaultFormats
    read[Map[String, ManifestEntry]](manifestJson)
  }

  def getBatchFromDeltaLog(deltaLog: Path): GwBatch = {
    val fs = FileSystem.get(SparkSession.active.sparkContext.hadoopConfiguration)
    val logContent = IOUtils.toString(fs.open(deltaLog), StandardCharsets.UTF_8)
    val version = GuidewireUtils.getVersionFromDeltaFileName(deltaLog.getName)
    val allFiles = GuidewireUtils.readAddFilesFromDeltaLog(logContent)
    val commitInfo = GuidewireUtils.getCommitInfo(logContent)
    // We do not really care of committed schema, we just want to know what files were added
    val schema = if (commitInfo.get.operationParameters.getOrElse("mode", "Nil") == "Overwrite") {
      Some(GwSchema("{}", commitInfo.get.timestamp))
    } else None: Option[GwSchema]
    require(commitInfo.isDefined, "Could not find commit information")
    GwBatch(
      commitInfo.get.timestamp,
      allFiles,
      schema = schema,
      version = version
    )
  }

  def getVersionFromDeltaFileName(deltaLogPath: String): Int = {
    deltaLogPath.split("\\.").head.toInt
  }

  def getCommitInfo(deltaLogJson: String): Option[GwCommit] = {
    implicit val formats: DefaultFormats.type = DefaultFormats
    deltaLogJson.split("\n").flatMap(line => {
      Try(read[Map[String, GwCommit]](line)).toOption
    }).flatMap(_.values).headOption
  }

  def readAddFilesFromDeltaLog(deltaLogJson: String): Array[GwFile] = {
    implicit val formats: DefaultFormats.type = DefaultFormats
    deltaLogJson.split("\n").flatMap(line => {
      Try(read[Map[String, GwFile]](line)).toOption
    }).map(_.head).filter(_._1 == "add").map(_._2)
  }

  def readSchema(content: Array[Byte]): String = {
    val parquetFile = new ParquetStreamScala(content)
    val parquetReader: ParquetReader[GenericRecord] = AvroParquetReader.builder[GenericRecord](parquetFile).build
    val avroSchema: Schema = parquetReader.read.getSchema
    SchemaConverters.toSqlType(avroSchema).dataType.asInstanceOf[StructType].json
  }


}
