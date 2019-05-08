package drivers

import config.AppConfig
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
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
    val newData = data.transform(transformer)
    sink.put(appConfig, newData)
  }

  private def transformer(in: DataFrame): DataFrame = {
    val isEven = functions.udf((i: Int) => i % 2 == 0)
    in.withColumn("account_verified", isEven(in("amount")))
  }
}
