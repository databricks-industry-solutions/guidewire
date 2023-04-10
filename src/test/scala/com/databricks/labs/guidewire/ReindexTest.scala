package com.databricks.labs.guidewire

import io.delta.tables.DeltaTable
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers

import java.nio.file.Files

class ReindexTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  Logger.getLogger("org.apache").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  override def beforeAll(): Unit = {
    SparkSession.builder()
      .master("local[*]")
      .appName("Guidewire")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()
  }
  override def afterAll(): Unit = {
    SparkSession.getActiveSession.foreach(_.close())
  }

  test("Reindex all guidewire") {
    val tmpDir = Files.createTempDirectory("guidewire")
    val manifestLocation = this.getClass.getResource("/guidewire/manifest.json")
    GuidewireSpark.reIndex(manifestLocation.toString, tmpDir.toString, SaveMode.Overwrite)
    SparkSession.active.read.format("delta").load(s"$tmpDir/databricks").show()
    DeltaTable.forPath(s"$tmpDir/databricks").history().show()
    DeltaTable.forPath(s"$tmpDir/databricks").toDF.write.mode("overwrite").format("delta").save(s"$tmpDir/databricks")
    println(s"$tmpDir/databricks")
  }

  {"add":{"path":"part-00008-324731ac-7da9-4978-8072-7e5a083b5804-c000.snappy.parquet","partitionValues":{},"size":903,"modificationTime":1681019312108,"dataChange":true,"stats":"{\"numRecords\":1,\"minValues\":{\"firstName\":\"Eon\",\"age\":35},\"maxValues\":{\"firstName\":\"Eon\",\"age\":35},\"nullCount\":{\"firstName\":0,\"lastName\":1,\"age\":0}}"}}
  {"remove":{"path":"file:/Users/antoine.amend/Workspace/guidewire/guidewire-db/src/test/resources/guidewire/databricks/301248660/1562112543752/part-00002-e0528fb1-5089-45bf-9de8-ef58140fa35f-c000.snappy.parquet","deletionTimestamp":1681019312626,"dataChange":true,"extendedFileMetadata":true,"partitionValues":{},"size":978}}
  {"commitInfo":{"timestamp":1681019312649,"operation":"WRITE","operationParameters":{"mode":"Overwrite","partitionBy":"[]"},"readVersion":3,"isolationLevel":"Serializable","isBlindAppend":false,"operationMetrics":{"numFiles":"9","numOutputRows":"16","numOutputBytes":"8636"},"engineInfo":"Apache-Spark/3.2.1 Delta-Lake/2.0.0","txnId":"3e819e84-c026-4d8f-8a72-d68132ba2121"}}


}
