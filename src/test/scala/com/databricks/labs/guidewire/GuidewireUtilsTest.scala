package com.databricks.labs.guidewire

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers

import java.io.InputStream

class GuidewireUtilsTest extends AnyFunSuite with Matchers {

  test("Deserializing manifest") {
    val manifestStream: InputStream = this.getClass.getResourceAsStream("/manifest.json")
    require(manifestStream != null)
    val map = GuidewireUtils.readManifest(manifestStream)
    map.keys must contain("databricks")
    map("databricks").dataFilesPath must be("databricks")
  }

  test("Generate file name") {
    GuidewireUtils.generateFileName(2).replace(".json", "").length must be(20)
    GuidewireUtils.generateFileName(212).replace(".json", "").length must be(20)
    GuidewireUtils.generateFileName(212311).replace(".json", "").length must be(20)
    assertThrows[IllegalArgumentException] {
      GuidewireUtils.generateFileName(-1)
    }
  }

}
