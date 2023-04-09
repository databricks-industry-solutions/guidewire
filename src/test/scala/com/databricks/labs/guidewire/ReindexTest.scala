package com.databricks.labs.guidewire

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers

import java.nio.file.Files

class ReindexTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  Logger.getLogger("org.apache").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  override def beforeAll(): Unit = {
    SparkSession.builder().master("local[*]").appName("Guidewire").getOrCreate()
  }
  override def afterAll(): Unit = {
    SparkSession.getActiveSession.foreach(_.close())
  }

  test("Reindex all guidewire") {
    val tmpDir = Files.createTempDirectory("guidewire")
    val manifestLocation = this.getClass.getResource("/guidewire/manifest.json")
    GuidewireSpark.reIndex(manifestLocation.toString, tmpDir.toString)
    SparkSession.active.read.format("delta").load(s"$tmpDir/databricks").show()
  }

}
