package com.databricks.labs.guidewire

import io.delta.standalone.{DeltaLog, Operation}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.avro.AvroParquetReader
import org.apache.parquet.hadoop.ParquetReader
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers
import io.delta.standalone.actions.{Action, AddFile, Metadata}
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll

import collection.JavaConverters._
import java.io.{File, FileInputStream, InputStream}
import java.util

class DeltaStandaloneTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  override def beforeAll(): Unit = {
    SparkSession.builder()
      .master("local[*]")
      .appName("Guidewire")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()
  }

  override def afterAll(): Unit = SparkSession.getActiveSession.foreach(_.close())

  test("Processing schemas") {

    val conf = new Configuration()

    // Get Parquet schema
    val parquetStream: InputStream = new FileInputStream(new File("/Users/antoine.amend/Workspace/guidewire/guidewire-db/src/test/resources/guidewire/policy_holders/301248659/1680350543000/part-00000-252b58d7-c77a-4c2e-8546-6bfe08e83721-c000.snappy.parquet"))
    val parquetBytes = IOUtils.toByteArray(parquetStream)
    val parquetReader: ParquetReader[GenericRecord] = AvroParquetReader.builder[GenericRecord](new ParquetStream(parquetBytes)).build
    val avroSchema: Schema = parquetReader.read.getSchema

    // Add Files
    val jmap = new util.HashMap[String, String]()
    val path = new File("/Users/antoine.amend/Workspace/guidewire/guidewire-db/src/test/resources/guidewire/policy_holders/301248659/1680350543000/part-00000-252b58d7-c77a-4c2e-8546-6bfe08e83721-c000.snappy.parquet")
    val files = List(
      new AddFile("s3://db-industry-gtm/fsi/solutions/guidewire/policy_holders/301248659/1680350543000/part-00000-252b58d7-c77a-4c2e-8546-6bfe08e83721-c000.snappy.parquet", jmap, path.length(), path.lastModified(), true, null, jmap),
    ).asJava

    // First transaction / new table with schema and files
    val log = DeltaLog.forTable(conf, "delta")
    val ops = new Operation(Operation.Name.WRITE)
    val txn = log.startTransaction()
    txn.updateMetadata(Metadata.builder().schema(avroSchema.convertToDelta).build())
    txn.commit(files, ops, "Guidewire-Connector/1.1.0")



  }

}
