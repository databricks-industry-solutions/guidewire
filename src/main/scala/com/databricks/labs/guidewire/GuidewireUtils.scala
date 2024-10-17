package com.databricks.labs.guidewire

import com.amazonaws.services.s3.AmazonS3URI
import io.delta.standalone.actions.{AddFile, Metadata}
import io.delta.standalone.types.StructType
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.commons.io.IOUtils
import org.apache.parquet.avro.AvroParquetReader
import org.apache.parquet.hadoop.ParquetReader
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.read
import org.slf4j.{Logger, LoggerFactory}

import java.io.InputStream
import java.nio.charset.StandardCharsets
import scala.annotation.tailrec

object GuidewireUtils {

  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def readManifest(manifestStream: InputStream): Map[String, ManifestEntry] = {
    val text: String = IOUtils.toString(manifestStream, StandardCharsets.UTF_8.name)
    readManifest(text)
  }

  def readManifest(manifestJson: String): Map[String, ManifestEntry] = {
    implicit val formats: DefaultFormats.type = DefaultFormats
    read[Map[String, ManifestEntry]](manifestJson)
  }

  @tailrec
  def readSchemaFromFiles(s3: S3Access, bucket: String, files: Array[AddFile]): Option[Metadata] = {
    if (files.isEmpty) {
      logger.error("Could not find any non empty file to read schema from")
      None: Option[Metadata]
    } else {
      val sampleFile = files.head
      val fileKey = new AmazonS3URI(sampleFile.getPath).getKey
      val sampleSchema = GuidewireUtils.readSchema(s3.readByteArray(bucket, fileKey))
      if (sampleSchema.isDefined) {
        Some(Metadata.builder().schema(sampleSchema.get).build())
      } else {
        logger.warn("Could not derive schema from file [{}]", fileKey)
        readSchemaFromFiles(s3, bucket, files.tail)
      }
    }
  }

  private def readSchema(content: Array[Byte]): Option[StructType] = {
    val parquetFile = new ParquetStream(content)
    val parquetReader: ParquetReader[GenericRecord] = AvroParquetReader.builder[GenericRecord](parquetFile).build
    val record: GenericRecord = parquetReader.read
    if (record == null) {
      logger.warn("Datafile seems empty, we won't be able to infer parquet schema")
      None: Option[StructType]
    } else {
      val avroSchema: Schema = record.getSchema
      Some(avroSchema.convertToDelta)
    }
  }

}
