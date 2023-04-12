package com.databricks.labs.guidewire

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers

import java.io.InputStream

class GuidewireUtilsTest extends AnyFunSuite with Matchers {

  test("Generate file name") {
    GuidewireUtils.generateFileName(2).replace(".json", "").length must be(20)
    GuidewireUtils.generateFileName(212).replace(".json", "").length must be(20)
    GuidewireUtils.generateFileName(212311).replace(".json", "").length must be(20)
    assertThrows[IllegalArgumentException] {
      GuidewireUtils.generateFileName(-1)
    }
  }

  test("Accumulating files to delete") {

    val newSchema = Some(GwSchema("{\"foo\":\"bar\"}", System.currentTimeMillis()))

    val batches = List(
      GwBatch(1L, version = 1, filesToAdd = Array(GwFile("foo1", 1, 1), GwFile("bar1", 1, 1))),
      GwBatch(2L, version = 2, filesToAdd = Array(GwFile("foo2", 2, 2), GwFile("bar2", 2, 2))),
      // At batch 3, we change schema. All files prior to batch 3 must be unregistered
      GwBatch(3L, version = 3, filesToAdd = Array(GwFile("foo3", 3, 3), GwFile("bar3", 3, 3)), schema = newSchema),
      GwBatch(4L, version = 4, filesToAdd = Array(GwFile("foo4", 4, 4), GwFile("bar4", 4, 4))),
      GwBatch(5L, version = 5, filesToAdd = Array(GwFile("foo5", 5, 5), GwFile("bar5", 5, 5))),
      // At batch 6, we change schema. All files prior to batch 6 and after batch 3 must be unregistered
      GwBatch(6L, version = 6, filesToAdd = Array(GwFile("foo6", 6, 6), GwFile("bar6", 6, 6)), schema = newSchema),
    )

    GuidewireUtils.accumulateAddFiles(batches).foreach(gw => {
      val fr = gw.filesToRemove.map(_.path).toSet
      gw.version match {
        case 1 => fr must be(empty)
        case 2 => fr must be(empty)
        case 3 => fr must be(Set("foo1", "foo2", "bar1", "bar2"))
        case 4 => fr must be(empty)
        case 5 => fr must be(empty)
        case 6 => fr must be(Set("foo3", "foo4", "foo5", "bar3", "bar4", "bar5"))
      }
    })

    GuidewireUtils.accumulateAddFiles(List.empty[GwBatch]) must be(empty)
    val emptyAccumulation = GuidewireUtils.accumulateAddFiles(batches.map(_.copy(schema = None)))
    emptyAccumulation.filter(_.filesToRemove.length > 0) must be(empty)
  }


  test("Deserializing manifest") {
    val manifestStream: InputStream = this.getClass.getResourceAsStream("/manifest.json")
    require(manifestStream != null)
    val map = GuidewireUtils.readManifest(manifestStream)
    map.keys must contain("databricks")
    map("databricks").dataFilesPath must be("databricks")
  }

}
