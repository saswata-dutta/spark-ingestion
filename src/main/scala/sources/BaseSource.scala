package sources

import config.AppConfig
import org.apache.spark.sql.{DataFrame, SparkSession}
import util.Logging

trait BaseSource extends Logging {
  def get(spark: SparkSession, config: AppConfig): DataFrame
}

object BaseSource {
  def apply(config: AppConfig): BaseSource = {
    val sinkType: String = config.conf.getConfig("sink").getString("type")
    sinkType match {
      case "dummy" => DummySource
    }
  }
}
