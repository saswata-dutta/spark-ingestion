package com.saswata.sparkseed.util

import org.scalatest.FlatSpec

class StringsSpec extends FlatSpec {
  it should "sanitise col names" in {
    val sanitised = Strings.sanitise("""   __a   ab'x --aA9"8%@#$^&*()  _""")
    assert(sanitised === """_a_ab_x_aA9_8_""")
    val sanitised_id = Strings.sanitise("_id_")
    assert(sanitised_id === "_id_")
  }
}
