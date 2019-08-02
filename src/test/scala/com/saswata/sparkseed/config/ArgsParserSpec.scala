package com.saswata.sparkseed.config

import com.saswata.sparkseed.config.AppConfig.KeyNames.LOCAL
import com.typesafe.config.ConfigFactory
import org.scalatest.FlatSpec

class ArgsParserSpec extends FlatSpec {

  private val defaultConfig = ConfigFactory.load()

  it should "tell isLocal" in {
    assert(new AppConfig(defaultConfig, Map(LOCAL    -> "true")).isLocal === true)
    assert(new AppConfig(defaultConfig, Map(LOCAL    -> "false")).isLocal === false)
    assert(new AppConfig(defaultConfig, Map("--blah" -> "true")).isLocal === false)
  }

  it should "guess stop Epoch" in {
    assert(
      new AppConfig(defaultConfig, Map("--stop-epoch" -> "1557599400")).stopTime === 1557599400000L
    )

    assert(new AppConfig(defaultConfig, Map("--stop-epoch" -> "1557599400000")).stopTime === 0L)
  }

  it should "guess start Date" in {
    assert(
      new AppConfig(defaultConfig, Map("--start-date" -> "2019-05-12")).startTime === 1557599400000L
    )
  }

}
