package com.saswata.sparkseed.transforms

import org.scalatest.FunSuite

class UdfsTest extends FunSuite {
  test("testStrSanitiser") {
    assert(Udfs.Funcs.strSanitiserFn(null) === "null")
    assert(Udfs.Funcs.strSanitiserFn("_id_") === "id")
    assert(Udfs.Funcs.strSanitiserFn("") === "")
  }

  test("testToEpochMillis") {
    assert(Udfs.Funcs.toEpochMsFn("", 1564736389.08013) === 1564736389080L)

    val smallEpoch = 156473638L
    val thrown = intercept[IllegalArgumentException](Udfs.Funcs.toEpochMsFn("", smallEpoch))
    assert(thrown.getMessage contains "Failed to convert Epoch")
    assert(thrown.getMessage contains s"$smallEpoch")
  }

  test("testTimePartition") {
    val epoch = 156473638908013L
    assert(Udfs.Funcs.timePartitionFn(epoch) === (("2019", "08", "02")))
  }

  test("testIsDouble") {
    assert(Udfs.Funcs.testDouble(null) === false)
    assert(Udfs.Funcs.testDouble("") === false)
    assert(Udfs.Funcs.testDouble("12a") === false)
    assert(Udfs.Funcs.testDouble("12.3") === true)
    assert(Udfs.Funcs.testDouble(0) === true)
    assert(Udfs.Funcs.testDouble(Double.MaxValue) === true)
  }

  test("testIsBool") {
    assert(Udfs.Funcs.testBool(null) === false)
    assert(Udfs.Funcs.testBool("") === false)
    assert(Udfs.Funcs.testBool("12a") === false)
    assert(Udfs.Funcs.testBool("false") === true)
    assert(Udfs.Funcs.testBool("False") === true)
    assert(Udfs.Funcs.testBool(false) === true)
    assert(Udfs.Funcs.testBool("True") === true)
    assert(Udfs.Funcs.testBool("true") === true)
    assert(Udfs.Funcs.testBool(true) === true)
  }

}
