package config

import org.scalatest.FlatSpec

class ArgsParserSpec extends FlatSpec {

  it should "tell isLocal" in {
    assert(new AppConfig(null, Map("--local" -> "true")).isLocal === true)
    assert(new AppConfig(null, Map("--local" -> "yes")).isLocal === false)
    assert(new AppConfig(null, Map("--blah"  -> "true")).isLocal === false)
  }

  it should "guess stop Epoch" in {
    assert(
      new AppConfig(null, Map("--stop-epoch" -> "1557599400")).stopTime("ms") === 1557599400000L
    )

    assert(new AppConfig(null, Map("--stop-epoch" -> "1557599400000")).stopTime("ms") === 0L)
  }

  it should "guess start Date" in {
    assert(
      new AppConfig(null, Map("--start-date" -> "2019-05-12")).startTime("ms") === 1557599400000L
    )
  }

}
