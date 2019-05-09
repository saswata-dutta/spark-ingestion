package sources

import config.AppConfig
import org.apache.spark.sql.{DataFrame, SparkSession}
import util.Logging

trait BaseSource extends Logging {
  def get(spark: SparkSession, config: AppConfig): DataFrame
}

object BaseSource {

  def apply(config: AppConfig): BaseSource = {
    val sourceType: String = config.conf.getConfig("source").getString("type")
    sourceType match {
      case "dummy"   => DummySource
      case "mongodb" => MongoSource
    }
  }
}
