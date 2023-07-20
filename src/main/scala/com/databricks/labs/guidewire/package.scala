package com.databricks.labs

import com.databricks.labs.guidewire.ParquetUtils.toSqlTypeHelper
import io.delta.standalone.actions.{AddFile, Metadata}
import io.delta.standalone.types.StructType
import org.apache.avro.Schema

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

  implicit class AvroSchemaImpl(schema: Schema) {
    def convertToDelta: StructType = {
      toSqlTypeHelper(schema).dataType.asInstanceOf[StructType]
    }
  }

  case class Batch(
                    schemaId: String,
                    commitTimestamp: Long,
                    filesToAdd: Seq[AddFile],
                    metadata: Option[Metadata]
                  )

  case class BatchResult(
                          schemaId: String,
                          commitTimestamp: Long,
                          numFiles: Int
                        )

}
