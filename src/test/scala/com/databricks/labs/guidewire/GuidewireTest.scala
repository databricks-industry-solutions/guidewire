package com.databricks.labs.guidewire

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers

import java.io.{File, FileFilter}
import java.nio.file.Files

class GuidewireTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

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
        BatchResult("121", 1L, 1),
        BatchResult("122", 2L, 2),
        BatchResult("123", 3L, 3),
      ),
      "bar" -> List(
        BatchResult("121", 1L, 1),
        BatchResult("122", 2L, 2),
        BatchResult("123", 3L, 3),
      )
    )
    Guidewire.saveCheckpoints(batches, tempDir.toString, SaveMode.Overwrite)
    val loadedCheckpoints = Guidewire.loadCheckpoints(tempDir.toString)
    loadedCheckpoints must be(Map("foo" -> 3, "bar" -> 3))
  }

  test("Reading empty checkpoints") {
    val tempDir = Files.createTempDirectory("guidewire_empty")
    val loadedCheckpoints = Guidewire.loadCheckpoints(tempDir.toString)
    loadedCheckpoints must be(empty)
  }

  test("Read delta log") {
    val deltaLogUrl = this.getClass.getResource("/delta/00000000000000000002.json")
    val deltaLogDir = deltaLogUrl.toString
    val extractedBatch = GuidewireUtils.getBatchFromDeltaLog(new Path(deltaLogDir))
    extractedBatch.version must be(2)
    extractedBatch.timestamp must be(1562112543751L)
    extractedBatch.schema must not be empty
    extractedBatch.filesToAdd.length must be(1)
  }

  test("Read delta log without schema") {
    val deltaLogUrl = this.getClass.getResource("/delta/00000000000000000001.json")
    val deltaLogDir = deltaLogUrl.toString
    val extractedBatch = GuidewireUtils.getBatchFromDeltaLog(new Path(deltaLogDir))
    extractedBatch.schema must be(empty)
  }

  test("save delta log") {
    val hdp = SparkSession.active.sparkContext.hadoopConfiguration
    val pathFilter = new FileFilter {
      override def accept(pathname: File): Boolean = {
        !pathname.getName.startsWith(".") && pathname.getName.endsWith(".json")
      }
    }
    val tempDir = Files.createTempDirectory("delta_log")
    val deltaFileName = "foo" + File.separator + "_delta_log"
    val batchesInit = List(
      GwBatch(0L, schemaId = "123", filesToAdd = Array.empty[GwFile]),
      GwBatch(1L, schemaId = "123", filesToAdd = Array.empty[GwFile], version = 1),
      GwBatch(2L, schemaId = "123", filesToAdd = Array.empty[GwFile], version = 2),
    )
    Guidewire.saveDeltaLogOverwrite(hdp, "foo", batchesInit, tempDir.toString)
    val filesInit = new File(tempDir.toFile, deltaFileName).listFiles(pathFilter)
    filesInit.length must be(3)

    val batchesOverwrite = List(
      GwBatch(0L, schemaId = "123", filesToAdd = Array.empty[GwFile]),
      GwBatch(1L, schemaId = "123", filesToAdd = Array.empty[GwFile], version = 1),
    )
    Guidewire.saveDeltaLogOverwrite(hdp, "foo", batchesOverwrite, tempDir.toString)
    val filesOverwrite = new File(tempDir.toFile, deltaFileName).listFiles(pathFilter)
    filesOverwrite.length must be(2)

    val batchesAppend = List(
      GwBatch(0L, schemaId = "123", filesToAdd = Array.empty[GwFile]),
      GwBatch(1L, schemaId = "123", filesToAdd = Array.empty[GwFile], version = 1),
      GwBatch(2L, schemaId = "123", filesToAdd = Array.empty[GwFile], version = 2),
    )
    Guidewire.saveDeltaLogAppend(hdp, "foo", batchesAppend, tempDir.toString)
    val filesAppend = new File(tempDir.toFile, deltaFileName).listFiles(pathFilter)
    filesAppend.foreach(println)
    filesAppend.length must be(5)
  }

  test("read delta log through spark") {
    val hdp = SparkSession.active.sparkContext.hadoopConfiguration
    val schema0 = StructType(Seq(StructField("foo", StringType, nullable = true)))
    val schema1 = StructType(Seq(StructField("foo", StringType, nullable = true), StructField("bar", StringType, nullable = true)))
    val schema2 = StructType(Seq(StructField("foo", StringType, nullable = true), StructField("bar", StringType, nullable = true), StructField("helloWorld", IntegerType, nullable = true)))
    val batchesAppend = List(
      GwBatch(0L, schemaId = "123", filesToAdd = Array.empty[GwFile], schema = Some(GwSchema(schema0.json, 0L))),
      GwBatch(1L, schemaId = "123", filesToAdd = Array.empty[GwFile], version = 1, schema = Some(GwSchema(schema1.json, 1L))),
      GwBatch(2L, schemaId = "123", filesToAdd = Array.empty[GwFile], version = 2, schema = Some(GwSchema(schema2.json, 2L))),
    )
    val tempDir = Files.createTempDirectory("delta_log_spark")
    Guidewire.saveDeltaLogOverwrite(hdp, "foo", batchesAppend, tempDir.toString)

    SparkSession.active
      .read
      .format("delta")
      .option("versionAsOf", 0)
      .load(new File(tempDir.toFile, "foo").toString)
      .schema must be(schema0)

    SparkSession.active
      .read
      .format("delta")
      .option("versionAsOf", 1)
      .load(new File(tempDir.toFile, "foo").toString)
      .schema must be(schema1)

    SparkSession.active
      .read
      .format("delta")
      .option("versionAsOf", 2)
      .load(new File(tempDir.toFile, "foo").toString)
      .schema must be(schema2)

  }

}
