package sinks
import config.AppConfig
import org.apache.spark.sql.{DataFrame, SparkSession}

object DummySink extends BaseSink {
  override def put(spark: SparkSession,
                   config: AppConfig,
                   data: DataFrame): Boolean = {
    data.foreach(it => println(it))
    true
  }
}
