package com.databricks.labs.guidewire

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers

import java.io.InputStream
import java.nio.file.Paths
import java.util.Objects

class GuidewireUtilsTest extends AnyFunSuite with Matchers {

  test("Deserializing manifest") {
    val manifestStream: InputStream = this.getClass.getResourceAsStream("/manifest.json")
    require(manifestStream != null)
    val map = GuidewireUtils.readManifest(manifestStream)
    map.keys must contain("databricks")
    map("databricks").dataFilesPath must be("databricks")
  }

  test("Generate file name") {
    GuidewireUtils.generateFileName(2).length must be(20 + 5)
    GuidewireUtils.generateFileName(212).length must be(20 + 5)
    GuidewireUtils.generateFileName(212311).length must be(20 + 5)
  }

  test("Schema parquet") {
    val parquetLocation = this.getClass.getResource("/example.snappy.parquet")
    val parquetFile = new LocalInputFile(Paths.get(Objects.requireNonNull(parquetLocation).toURI))
    val parquetSchema = GuidewireUtils.readSchema(parquetFile).json
    val expected = "{\"type\":\"struct\",\"fields\":" + "[{\"name\":\"firstName\",\"type\":\"string\"," +
      "\"nullable\":true,\"metadata\":{}}," + "{\"name\":\"lastName\",\"type\":\"string\",\"nullable\":true," +
      "\"metadata\":{}},{\"name\":\"age\"," + "\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}"
    parquetSchema must be(expected)
  }

}
