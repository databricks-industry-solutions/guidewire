package com.databricks.labs

import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import java.util.UUID

package object guidewire {

  case class ManifestEntry(
                            lastSuccessfulWriteTimestamp: String,
                            totalProcessedRecordsCount: Int,
                            dataFilesPath: String,
                            schemaHistory: Map[String, String]
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
              ("deletionTimestamp" -> modificationTime)
            )
        case _ => throw new IllegalArgumentException(s"unsupported file operation [$operation]")
      }
      compact(render(json))
    }

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

  case class GwBatch(
                      timestamp: Long,
                      files: Array[GwFile],
                      txnId: String = UUID.randomUUID().toString,
                      schema: Option[GwSchema] = None,
                      version: Int = 0,
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

      files
        .map(_.copy(operation = "add"))
        .sortBy(_.modificationTime)
        .map(_.toJson)
        .foreach(j => {
          sb.append(j)
          sb.append("\n")
        })

      val totalBytes = files.map(_.size).sum
      val totalFiles = files.length

      sb.append(compact(render("commitInfo" -> (
        ("timestamp" -> timestamp) ~
          ("operation" -> "WRITE") ~
          ("operationParameters" -> (
            ("mode" -> "Append") ~
              ("partitionBy" -> "[]")
            )) ~
          ("isolationLevel" -> "Serializable") ~
          ("operationMetrics" -> (
            ("numFiles" -> totalFiles) ~
//            ("numOutputRows" -> 16) ~
            ("numOutputBytes" -> totalBytes)
          )) ~
          ("isBlindAppend" -> true) ~

          ("txnId" -> txnId)
        ))))

      sb.toString()

    }

  }

}
