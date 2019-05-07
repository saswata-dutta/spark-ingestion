package config

import com.typesafe.config.{Config, ConfigFactory}

/**
  * Inspired by https://github.com/typesafehub/config/blob/master/examples/scala/simple-lib/src/main/scala/simplelib/SimpleLib.scala
  *
  * Fail fast when dealing with config values!
  */
class AppConfig(config: Config) {

  //use default config on classpath if no config is passed here
  def this(){
    this(ConfigFactory.load())
  }

  val master = config.getString("spark.master")
}
