package drivers

import config.AppConfig
import org.apache.spark.sql.{DataFrame, SparkSession}
import sinks.BaseSink
import sources.BaseSource

object TestJob extends BaseDriver {

  override def run(
    spark: SparkSession,
    appConfig: AppConfig,
    source: BaseSource,
    sink: BaseSink
  ): Boolean = {
    val data: DataFrame = source.get(spark, appConfig)
    sink.put(appConfig, data)
  }
}
