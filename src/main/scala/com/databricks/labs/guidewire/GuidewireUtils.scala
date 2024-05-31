package com.databricks.labs.guidewire

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

  def readSchema(content: Array[Byte]): Option[StructType] = {
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
