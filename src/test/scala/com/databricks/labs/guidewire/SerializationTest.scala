package com.databricks.labs.guidewire

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers

class SerializationTest extends AnyFunSuite with Matchers {

  test("Serializing guidewire file") {
    GwFile(
      path = "/path/to/file",
      size = 12,
      modificationTime = 1234567890,
    ).toJson must be("""{"add":{"path":"/path/to/file","size":12,"modificationTime":1234567890}}""")

    println(GwFile(
      path = "/path/to/file",
      size = 12,
      modificationTime = 1234567890,
      operation = "remove"
    ))
    GwFile(
      path = "/path/to/file",
      size = 12,
      modificationTime = 1234567890,
      operation = "remove"
    ).toJson must be("""{"remove":{"path":"/path/to/file","deletionTimestamp":1234567890}}""")

    assertThrows[IllegalArgumentException] {
      GwFile(
        path = "/path/to/file",
        size = 12,
        modificationTime = 1234567890,
        operation = "INVALID"
      ).toJson
    }
  }

  test("Serializing guidewire schema") {
    GwSchema(
      id = "123456",
      schema = "{\"type\":\"struct\",\"fields\":" + "[{\"name\":\"firstName\",\"type\":\"string\"," +
        "\"nullable\":true,\"metadata\":{}}," + "{\"name\":\"lastName\",\"type\":\"string\",\"nullable\":true," +
        "\"metadata\":{}},{\"name\":\"age\"," + "\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}",
      createdTime = 1234567890
    ).toJson must be("{\"metaData\":{\"id\":\"123456\",\"format\":{\"provider\":\"parquet\",\"options\":{}}," +
      "\"schemaString\":\"{\\\"type\\\":\\\"struct\\\",\\\"fields\\\":[{\\\"name\\\":\\\"firstName\\\"," +
      "\\\"type\\\":\\\"string\\\",\\\"nullable\\\":true,\\\"metadata\\\":{}},{\\\"name\\\":\\\"lastName\\\"," +
      "\\\"type\\\":\\\"string\\\",\\\"nullable\\\":true,\\\"metadata\\\":{}},{\\\"name\\\":\\\"age\\\"," +
      "\\\"type\\\":\\\"integer\\\",\\\"nullable\\\":true,\\\"metadata\\\":{}}]}\",\"partitionColumns\":[]," +
      "\"createdTime\":1234567890}}")
  }

  test("Serializing guidewire batch") {
    val schema = GwSchema(
      id = "123456",
      schema = "{\"type\":\"struct\",\"fields\":" + "[{\"name\":\"firstName\",\"type\":\"string\"," +
        "\"nullable\":true,\"metadata\":{}}," + "{\"name\":\"lastName\",\"type\":\"string\",\"nullable\":true," +
        "\"metadata\":{}},{\"name\":\"age\"," + "\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}",
      createdTime = 1234567890
    )
    val listAdd = Array(
      GwFile(
        path = "/path/to/add",
        size = 12,
        modificationTime = 1234567890,
      )
    )

    GwBatch(
      version = 0,
      timestamp = 1234567892,
      txnId = "0002b396-4334-4f06-8f6b-69ab6a65851a",
      filesToAdd = listAdd,
      schema = Some(schema)
    ).toJson must be(
      """{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}
        |{"metaData":{"id":"123456","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"firstName\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"lastName\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"age\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"createdTime":1234567890}}
        |{"add":{"path":"/path/to/add","size":12,"modificationTime":1234567890}}
        |{"commitInfo":{"timestamp":1234567892,"operation":"WRITE","operationParameters":{"mode":"Append","partitionBy":"[]"},"isolationLevel":"Serializable","isBlindAppend":true,"txnId":"0002b396-4334-4f06-8f6b-69ab6a65851a"}}""".stripMargin)

    GwBatch(
      version = 1,
      timestamp = 1234567892,
      txnId = "0002b396-4334-4f06-8f6b-69ab6a65851a",
      filesToAdd = listAdd,
      schema = Some(schema)
    ).toJson must be(
      """{"metaData":{"id":"123456","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"firstName\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"lastName\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"age\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"createdTime":1234567890}}
        |{"add":{"path":"/path/to/add","size":12,"modificationTime":1234567890}}
        |{"commitInfo":{"timestamp":1234567892,"operation":"WRITE","operationParameters":{"mode":"Append","partitionBy":"[]"},"isolationLevel":"Serializable","isBlindAppend":true,"txnId":"0002b396-4334-4f06-8f6b-69ab6a65851a"}}""".stripMargin)

    GwBatch(
      version = 2,
      timestamp = 1234567892,
      txnId = "0002b396-4334-4f06-8f6b-69ab6a65851a",
      filesToAdd = Array.empty,
      schema = Some(schema)
    ).toJson must be(
      """{"metaData":{"id":"123456","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"firstName\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"lastName\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"age\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"createdTime":1234567890}}
        |{"commitInfo":{"timestamp":1234567892,"operation":"WRITE","operationParameters":{"mode":"Append","partitionBy":"[]"},"isolationLevel":"Serializable","isBlindAppend":true,"txnId":"0002b396-4334-4f06-8f6b-69ab6a65851a"}}""".stripMargin)
  }

}
