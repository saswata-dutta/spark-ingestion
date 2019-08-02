package com.saswata.sparkseed.sinks

import com.saswata.sparkseed.config.AppConfig
import com.saswata.sparkseed.transforms.GenericTransformers
import com.saswata.sparkseed.util.Logging
import org.apache.spark.sql.DataFrame

trait BaseSink extends Logging {

  @SuppressWarnings(Array("org.wartremover.warts.DefaultArguments"))
  def put(
    appConfig: AppConfig,
    data: DataFrame,
    params: Map[String, String] = Map.empty[String, String]
  ): Boolean
}

object BaseSink {

  def apply(config: AppConfig): BaseSink = {
    val sinkType: String = config.sinkConfig.getString(AppConfig.KeyNames.TYPE)
    sinkType match {
      case AppConfig.KeyNames.DUMMY        => DummySink
      case AppConfig.KeyNames.S3           => S3Sink
      case AppConfig.KeyNames.S3_TIME_PART => S3TimePartSink
    }
  }

  def process(appConfig: AppConfig)(data: DataFrame): DataFrame = {
    val data1 =
      if (appConfig.flattenCols) GenericTransformers.flatten(data)
      else data

    data1
      .transform(GenericTransformers.dropNullCols)
      .transform(GenericTransformers.sanitiseColNames)
  }
}
