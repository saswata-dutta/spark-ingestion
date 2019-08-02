package com.saswata.sparkseed.sinks

import com.saswata.sparkseed.config.AppConfig
import org.apache.spark.sql.DataFrame

object DummySink extends BaseSink {
  val numShow = 10

  override def put(appConfig: AppConfig, data: DataFrame, params: Map[String, String]): Boolean = {
    // get owner spark session from DF itself
    //    val spark: SparkSession = data.sparkSession
    logger.info("Start Sink ...")
    data.explain(extended = true)
    val result = BaseSink.process(appConfig)(data)
    result.printSchema()
    result.show(numShow)

    true
  }
}
