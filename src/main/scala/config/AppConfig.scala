package config

import com.typesafe.config.{Config, ConfigFactory}
import util.IstTime

/**
  * Inspired by https://github.com/typesafehub/config/blob/master/examples/scala/simple-lib/src/main/scala/simplelib/SimpleLib.scala
  *
  * Fail fast when dealing with config values!
  */
final class AppConfig(val conf: Config, val args: Map[String, String]) {

  //use default config on classpath if no config is passed here
  def this() {
    this(ConfigFactory.load(), Map.empty[String, String])
  }

  def isLocal: Boolean = args.get("--local").contains("true")

  def startTime(unit: String): Long = convertTimeArg("start", unit)

  def stopTime(unit: String): Long = convertTimeArg("stop", unit)

  private def convertTimeArg(prefix: String, unit: String): Long =
    args
      .get(s"--$prefix-epoch")
      .filter(_.length == 10)
      .map(_.toLong)
      .orElse(args.get(s"--$prefix-date").map(d => IstTime.dateStrToEpochSecs(d)))
      .map(e => IstTime.scaleEpochSecs(e, unit))
      .getOrElse(0L)
}
