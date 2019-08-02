package com.saswata.sparkseed.drivers

import com.saswata.sparkseed.config.AppConfig
import com.saswata.sparkseed.sinks.BaseSink
import com.saswata.sparkseed.sources.BaseSource
import org.apache.spark.sql.functions.when
import org.apache.spark.sql.{DataFrame, SparkSession}

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

  private def transformer(df: DataFrame): DataFrame =
    df.withColumn("account_verified", when(df("amount") % 2 === false, false).otherwise(true))
}
