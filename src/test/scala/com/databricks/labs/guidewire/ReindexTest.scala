package com.databricks.labs.guidewire

import io.delta.tables.DeltaTable
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers

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

  override def afterAll(): Unit = SparkSession.getActiveSession.foreach(_.close())

  test("Reindex all guidewire") {
    val guidewire = new GuidewireSpark(region = Some("us-east-2"))
    val manifest = guidewire.readManifest("s3://aamend/dev/guidewire/manifest.json")
    val batches = guidewire.readBatches(manifest)
    guidewire.saveDeltaLog(batches, "/Users/antoine.amend/Workspace/guidewire/guidewire-db/spark")
  }

}
