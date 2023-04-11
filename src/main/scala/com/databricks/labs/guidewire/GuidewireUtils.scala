package com.databricks.labs.guidewire

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.commons.io.IOUtils
import org.apache.parquet.avro.AvroParquetReader
import org.apache.parquet.hadoop.ParquetReader
import org.apache.spark.sql.avro.SchemaConverters
import org.apache.spark.sql.types.StructType
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.read

import java.io.InputStream
import java.nio.charset.StandardCharsets

object GuidewireUtils {

  def generateFileName(version: Int): String = {
    f"$version%020d.json"
  }

  def readManifest(manifestStream: InputStream): Map[String, ManifestEntry] = {
    val text: String = IOUtils.toString(manifestStream, StandardCharsets.UTF_8.name)
    readManifest(text)
  }

  def readManifest(manifestJson: String): Map[String, ManifestEntry] = {
    implicit val formats: DefaultFormats.type = DefaultFormats
    read[Map[String, ManifestEntry]](manifestJson)
  }

  def readSchema(content: Array[Byte]): String = {
    val parquetFile = new ParquetStream(content)
    val parquetReader: ParquetReader[GenericRecord] = AvroParquetReader.builder[GenericRecord](parquetFile).build
    val avroSchema: Schema = parquetReader.read.getSchema
    SchemaConverters.toSqlType(avroSchema).dataType.asInstanceOf[StructType].json
  }

}
