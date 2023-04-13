package com.databricks.labs.guidewire

import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers

import java.io.{File, FileInputStream}
import java.nio.charset.StandardCharsets
import java.nio.file.Files

class GuidewireSparkIntegrationTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

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

  test("Serializing checkpoints") {
    val tempDir = Files.createTempDirectory("guidewire")
    val batches = Map(
      "foo" -> List(
        GwBatch(0L, Array.empty[GwFile]),
        GwBatch(1L, Array.empty[GwFile]),
        GwBatch(2L, Array.empty[GwFile]),
      ),
      "bar" -> List(
        GwBatch(1L, Array.empty[GwFile]),
        GwBatch(2L, Array.empty[GwFile]),
        GwBatch(3L, Array.empty[GwFile]),
      )
    )
    GuidewireSpark.saveCheckpoints(batches, tempDir.toString)
    val loadedCheckpoints = GuidewireSpark.loadCheckpoints(tempDir.toString)
    loadedCheckpoints must be(Map("foo" -> 2, "bar" -> 3))
  }

  test("Reading empty") {
    val tempDir = Files.createTempDirectory("guidewire_empty")
    val loadedCheckpoints = GuidewireSpark.loadCheckpoints(tempDir.toString)
    loadedCheckpoints must be(empty)
  }

  test("Read delta log") {
    val deltaLogUrl = this.getClass.getResource("delta/00000000000000000002.json")
    val deltaLogDir = deltaLogUrl.toString
    val extractedBatch = GuidewireUtils.getBatchFromDeltaLog(new Path(deltaLogDir))
    extractedBatch.version must be(2)
    extractedBatch.timestamp must be(1562112543751L)
    extractedBatch.schema must not be empty
    extractedBatch.filesToRemove.length must be(4)
    extractedBatch.filesToAdd.length must be(1)
  }

  test("Read delta log without schema") {
    val deltaLogUrl = this.getClass.getResource("delta/00000000000000000001.json")
    val deltaLogDir = deltaLogUrl.toString
    val extractedBatch = GuidewireUtils.getBatchFromDeltaLog(new Path(deltaLogDir))
    extractedBatch.schema must be(empty)
  }



























  test("Reindex all guidewire") {
    val manifest = GuidewireSpark.readManifest("s3://aamend/dev/guidewire/manifest.json")
    val batches = GuidewireSpark.processManifest(manifest)
    GuidewireSpark.saveDeltaLog(batches, "/Users/antoine.amend/Workspace/guidewire/guidewire-db/spark")
  }

  test("Parse deltaLog") {
    val log = "/Users/antoine.amend/Workspace/guidewire/guidewire-db/spark/databricks/_delta_log/00000000000000000000.json"
    val deltaJson = IOUtils.toString(new FileInputStream(new File(log)), StandardCharsets.UTF_8)
    GuidewireUtils.readAddFilesFromDeltaLog(deltaJson).foreach(println)
  }

  test("read delta log") {
    val spark = SparkSession.active
    import org.apache.spark.sql.functions._
    spark
      .read
      .format("json")
      .load("/Users/antoine.amend/Workspace/guidewire/guidewire-db/spark/databricks/_delta_log")
      .filter(col("add").isNotNull)
      .select(col("add.path"))
      .show()
  }

}
