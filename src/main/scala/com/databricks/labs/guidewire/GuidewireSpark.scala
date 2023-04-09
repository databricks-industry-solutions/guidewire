package com.databricks.labs.guidewire

import com.databricks.labs.guidewire.GuidewireUtils.readManifest
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path, PathFilter}
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

import java.nio.charset.Charset

object GuidewireSpark {

  val logger: Logger = LoggerFactory.getLogger(GuidewireSpark.getClass)

  def reIndex(manifestLocation: String, databasePath: String): Unit = {

    require(SparkSession.getActiveSession.isDefined, "A spark session must be enabled")
    val spark = SparkSession.active

    logger.info("Reading manifest file")
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val manifest = readManifest(fs.open(new Path(manifestLocation)))

    val guidewirePathFilter = new PathFilter {
      override def accept(path: Path): Boolean = {
        !path.getName.startsWith(".") && path.getName.contains(".parquet")
      }
    }

    val processBatch = (timestampPath: FileStatus, timestampId: Long) => {
      val committedTimestamp = timestampPath.getPath.getName.toLong
      val committedFiles = fs.listStatus(timestampPath.getPath, guidewirePathFilter).map(guidewireFile => {
        GwFile(
          guidewireFile.getPath.toString,
          guidewireFile.getLen,
          guidewireFile.getModificationTime
        )
      })
      val schema: Option[GwSchema] = if (timestampId == 0) {
        Some(GwSchema(
          GuidewireUtils.readSchema(HadoopInputFile.fromPath(new Path(committedFiles.head.path), fs.getConf)).json,
          committedTimestamp
        ))
      } else None: Option[GwSchema]
      GwBatch(committedTimestamp, committedFiles, schema = schema)
    }

    val processTable = (tableManifest: ManifestEntry) => {
      val tablePath = tableManifest.dataFilesPath
      tableManifest.schemaHistory.toList.sortBy(_._2.toLong).zipWithIndex.flatMap({ case ((schema, lastUpdated), schemaId) =>
        val schemaPath = new Path(tablePath, schema)
        val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
        fs.listStatus(schemaPath).filter(timestampPath => {
          timestampPath.getPath.getName.toLong <= lastUpdated.toLong
        }).sortBy(_.getPath.getName.toLong).zipWithIndex.map({ case (timestampPath, timestampId) =>
          (schemaId, timestampId, processBatch(timestampPath, timestampId))
        })
      }).sortBy({ case (schemaId, timestampId, _) =>
        (schemaId, timestampId)
      }).zipWithIndex.map({ case ((_, _, batch), commitId) =>
        batch.copy(version = commitId)
      })
    }

    manifest.par.map({ case (tableName, tableManifest) =>
      (tableName, processTable(tableManifest))
    }).foreach({ case (tableName, deltaFiles) =>
      val tablePath = new Path(databasePath, tableName)
      val deltaPath = new Path(tablePath, "_delta_log")
      if (fs.exists(deltaPath)) fs.delete(deltaPath, true)
      fs.mkdirs(deltaPath)
      deltaFiles.foreach(gwBatch => {
        val deltaFile = new Path(deltaPath, GuidewireUtils.generateFileName(gwBatch.version))
        val fos = fs.create(deltaFile)
        fos.write(gwBatch.toJson.getBytes(Charset.defaultCharset()))
        fos.close()
      })
    })







  }



}
