package com.databricks.labs

import com.amazonaws.services.s3.AmazonS3URI
import org.json4s.DefaultFormats
import org.json4s.JsonDSL._
import org.json4s.jackson.Json
import org.json4s.jackson.JsonMethods._

import java.util.UUID

package object guidewire {

  case class ManifestEntry(
                            lastSuccessfulWriteTimestamp: String,
                            totalProcessedRecordsCount: Int,
                            dataFilesPath: String,
                            schemaHistory: Map[String, String]
                          ) {
    def getDataFilesPath: String = {
      if (dataFilesPath.endsWith("/")) dataFilesPath.dropRight(1) else dataFilesPath
    }
  }

  case class BatchResult(
                          schemaId: String,
                          commitTimestamp: Long,
                          numFiles: Int
                        )

  case class GwFile(
                     path: String,
                     size: Long,
                     modificationTime: Long,
                     operation: String = "add"
                   ) {

    def toJson: String = {
      val json = operation match {
        case "add" =>
          "add" -> (
            ("path" -> path) ~
              ("size" -> size) ~
              ("partitionValues" -> Map.empty[String, String]) ~
              ("modificationTime" -> modificationTime) ~
              ("dataChange" -> true)
            )
        case "remove" =>
          "remove" -> (
            ("path" -> path) ~
              ("size" -> size) ~
              ("partitionValues" -> Map.empty[String, String]) ~
              ("deletionTimestamp" -> modificationTime) ~
              ("dataChange" -> true)
            )
        case _ => throw new IllegalArgumentException(s"unsupported file operation [$operation]")
      }
      compact(render(json))
    }

    def getKey: String = new AmazonS3URI(path).getKey

  }

  case class GwSchema(
                       schema: String,
                       createdTime: Long,
                       id: String = "schema"
                     ) {

    def toJson: String = {
      val json = "metaData" -> (
        ("id" -> id) ~
          ("format" -> (
            ("provider" -> "parquet") ~
              ("options" -> Map.empty[String, String])
            )) ~
          ("schemaString" -> schema) ~
          ("configuration" -> Map.empty[String, String]) ~
          ("partitionColumns" -> List.empty[String]) ~
          ("createdTime" -> createdTime)
        )
      compact(render(json))
    }

  }

  case class GwCommit(
                       timestamp: Long,
                       operation: String,
                       operationParameters: Map[String, String],
                       isolationLevel: String,
                       operationMetrics: Map[String, Long],
                       isBlindAppend: Boolean,
                       txnId: String
                     ) {

    def toJson: String = {
      Json(DefaultFormats).write(Map("commitInfo" -> this))
    }
  }

  case class GwBatch(
                      timestamp: Long,
                      filesToAdd: Array[GwFile],
                      filesToRemove: Array[GwFile] = Array.empty[GwFile],
                      txnId: String = UUID.randomUUID().toString,
                      schema: Option[GwSchema] = None,
                      version: Int = 0,
                      schemaId: String = "",
                    ) {

    def toJson: String = {

      val sb = new StringBuilder()

      if (version == 0) {
        sb.append(compact(render("protocol" -> (
          ("minReaderVersion" -> 1) ~
            ("minWriterVersion" -> 2)
          ))))
        sb.append("\n")
      }

      if (schema.isDefined) {
        sb.append(schema.get.toJson)
        sb.append("\n")
      }

      filesToAdd
        .map(_.copy(operation = "add"))
        .sortBy(_.modificationTime)
        .map(_.toJson)
        .foreach(j => {
          sb.append(j)
          sb.append("\n")
        })

      filesToRemove
        .map(_.copy(operation = "remove"))
        .sortBy(_.modificationTime)
        .map(_.toJson)
        .foreach(j => {
          sb.append(j)
          sb.append("\n")
        })

      val totalBytes = filesToAdd.map(_.size).sum
      val totalFiles = filesToAdd.length

      val commit = if (schema.isDefined) {
        GwCommit(
          timestamp = timestamp,
          operation = "WRITE",
          operationParameters = Map(
            "mode" -> "Overwrite",
            "partitionBy" -> "[]"
          ),
          isolationLevel = "Serializable",
          operationMetrics = Map(
            "numFiles" -> totalFiles,
            "numOutputBytes" -> totalBytes
          ),
          isBlindAppend = false,
          txnId = txnId
        )
      } else {
        GwCommit(
          timestamp = timestamp,
          operation = "WRITE",
          operationParameters = Map(
            "mode" -> "Append",
            "partitionBy" -> "[]"
          ),
          isolationLevel = "Serializable",
          operationMetrics = Map(
            "numFiles" -> totalFiles,
            "numOutputBytes" -> totalBytes
          ),
          isBlindAppend = true,
          txnId = txnId
        )
      }
      println(commit.toJson)
      sb.append(commit.toJson)
      sb.toString()
    }

  }

}
