package sinks

import config.AppConfig
import org.apache.spark.sql.DataFrame
import util.Logging

trait BaseSink extends Logging {

  def put(
    appConfig: AppConfig,
    data: DataFrame,
    params: Map[String, String] = Map.empty[String, String]
  ): Boolean
}

object BaseSink {

  def apply(config: AppConfig): BaseSink = {
    val sinkType: String = config.conf.getConfig("sink").getString("type")
    sinkType match {
      case "dummy" => DummySink
    }
  }
}
