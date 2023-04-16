//package com.databricks.labs.guidewire
//
//import org.json4s._
//import org.json4s.jackson.Serialization._
//import org.scalatest.funsuite.AnyFunSuite
//import org.scalatest.matchers.must.Matchers
//
//class EntityTest extends AnyFunSuite with Matchers {
//
//  test("Serializing guidewire file") {
//    GwFile(
//      path = "/path/to/file",
//      size = 12,
//      modificationTime = 1234567890,
//    ).toJson must be("""{"add":{"path":"/path/to/file","size":12,"partitionValues":{},"modificationTime":1234567890,"dataChange":true}}""")
//
//    GwFile(
//      path = "/path/to/file",
//      size = 12,
//      modificationTime = 1234567890,
//      operation = "remove"
//    ).toJson must be("""{"remove":{"path":"/path/to/file","size":12,"partitionValues":{},"deletionTimestamp":1234567890,"dataChange":true}}""")
//
//    assertThrows[IllegalArgumentException] {
//      GwFile(
//        path = "/path/to/file",
//        size = 12,
//        modificationTime = 1234567890,
//        operation = "INVALID"
//      ).toJson
//    }
//  }
//
//  test("Serializing guidewire schema") {
//    GwSchema(
//      id = "123456",
//      schema = "{\"type\":\"struct\",\"fields\":" + "[{\"name\":\"firstName\",\"type\":\"string\"," +
//        "\"nullable\":true,\"metadata\":{}}," + "{\"name\":\"lastName\",\"type\":\"string\",\"nullable\":true," +
//        "\"metadata\":{}},{\"name\":\"age\"," + "\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}",
//      createdTime = 1234567890
//    ).toJson must be("{\"metaData\":{\"id\":\"123456\",\"format\":{\"provider\":\"parquet\",\"options\":{}}," +
//      "\"schemaString\":\"{\\\"type\\\":\\\"struct\\\",\\\"fields\\\":[{\\\"name\\\":\\\"firstName\\\"," +
//      "\\\"type\\\":\\\"string\\\",\\\"nullable\\\":true,\\\"metadata\\\":{}},{\\\"name\\\":\\\"lastName\\\"," +
//      "\\\"type\\\":\\\"string\\\",\\\"nullable\\\":true,\\\"metadata\\\":{}},{\\\"name\\\":\\\"age\\\"," +
//      "\\\"type\\\":\\\"integer\\\",\\\"nullable\\\":true,\\\"metadata\\\":{}}]}\",\"configuration\":{}," +
//      "\"partitionColumns\":[],\"createdTime\":1234567890}}")
//  }
//
//  test("Serializing delta commit") {
//    val commit = GwCommit(
//      0L,
//      "append",
//      Map.empty,
//      "Serialization",
//      Map.empty,
//      isBlindAppend = true,
//      "tx"
//    )
//    val serialized = commit.toJson
//    implicit val formats: DefaultFormats.type = DefaultFormats
//    read[Map[String, GwCommit]](serialized).values.head must be(commit)
//  }
//
//  test("Serializing guidewire batch") {
//    val schema = GwSchema(
//      id = "123456",
//      schema = "{\"type\":\"struct\",\"fields\":" + "[{\"name\":\"firstName\",\"type\":\"string\"," +
//        "\"nullable\":true,\"metadata\":{}}," + "{\"name\":\"lastName\",\"type\":\"string\",\"nullable\":true," +
//        "\"metadata\":{}},{\"name\":\"age\"," + "\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}",
//      createdTime = 1234567890
//    )
//    val listAdd = Array(
//      GwFile(
//        path = "/path/to/add",
//        size = 12,
//        modificationTime = 1234567890,
//      )
//    )
//
//    GwBatch(
//      timestamp = 1234567892,
//      txnId = "0002b396-4334-4f06-8f6b-69ab6a65851a",
//      filesToAdd = listAdd,
//      schema = Some(schema)
//    ).toJson must be(
//      """{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}
//        |{"metaData":{"id":"123456","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"firstName\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"lastName\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"age\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}","configuration":{},"partitionColumns":[],"createdTime":1234567890}}
//        |{"add":{"path":"/path/to/add","size":12,"partitionValues":{},"modificationTime":1234567890,"dataChange":true}}
//        |{"commitInfo":{"timestamp":1234567892,"operation":"WRITE","operationParameters":{"mode":"Overwrite","partitionBy":"[]"},"isolationLevel":"Serializable","operationMetrics":{"numFiles":1,"numOutputBytes":12},"isBlindAppend":false,"txnId":"0002b396-4334-4f06-8f6b-69ab6a65851a"}}""".stripMargin)
//
//    GwBatch(
//      version = 1,
//      timestamp = 1234567892,
//      txnId = "0002b396-4334-4f06-8f6b-69ab6a65851a",
//      filesToAdd = listAdd,
//      schema = Some(schema)
//    ).toJson must be(
//      """{"metaData":{"id":"123456","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"firstName\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"lastName\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"age\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}","configuration":{},"partitionColumns":[],"createdTime":1234567890}}
//        |{"add":{"path":"/path/to/add","size":12,"partitionValues":{},"modificationTime":1234567890,"dataChange":true}}
//        |{"commitInfo":{"timestamp":1234567892,"operation":"WRITE","operationParameters":{"mode":"Overwrite","partitionBy":"[]"},"isolationLevel":"Serializable","operationMetrics":{"numFiles":1,"numOutputBytes":12},"isBlindAppend":false,"txnId":"0002b396-4334-4f06-8f6b-69ab6a65851a"}}""".stripMargin)
//
//    GwBatch(
//      version = 2,
//      timestamp = 1234567892,
//      txnId = "0002b396-4334-4f06-8f6b-69ab6a65851a",
//      filesToAdd = Array.empty,
//      schema = Some(schema)
//    ).toJson must be(
//      """{"metaData":{"id":"123456","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"firstName\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"lastName\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"age\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}","configuration":{},"partitionColumns":[],"createdTime":1234567890}}
//        |{"commitInfo":{"timestamp":1234567892,"operation":"WRITE","operationParameters":{"mode":"Overwrite","partitionBy":"[]"},"isolationLevel":"Serializable","operationMetrics":{"numFiles":0,"numOutputBytes":0},"isBlindAppend":false,"txnId":"0002b396-4334-4f06-8f6b-69ab6a65851a"}}""".stripMargin)
//  }
//
//}
