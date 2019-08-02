package com.saswata.sparkseed.drivers

import com.saswata.sparkseed.config.AppConfig
import com.saswata.sparkseed.sinks.BaseSink
import com.saswata.sparkseed.sources.BaseSource
import com.saswata.sparkseed.transforms.GenericTransformers
import com.saswata.sparkseed.util.IstTime
import org.apache.spark.sql.{DataFrame, SparkSession}

/*
--class com.saswata.sparkseed.drivers.DefaultIngestJob
--conf-file supply_user.conf
--local true
 */
object DefaultIngestJob extends BaseDriver {
  override def run(
    spark: SparkSession,
    appConfig: AppConfig,
    source: BaseSource,
    sink: BaseSink
  ): Boolean = {

    val epochCols = appConfig.epochCols
    logger.info(s"Will fix epochs : $epochCols")
    val numericCols = appConfig.numericCols
    logger.info(s"Will cast to Double $numericCols")
    val boolCols = appConfig.boolCols
    logger.info(s"Will cast to Boolean $boolCols")
    val partitionCols = appConfig.partitionCols
    logger.info(s"Will sanitise text in  $partitionCols")

    logger.info("Fetching...")
    val data: DataFrame = source.get(spark, appConfig)
    logger.info(s"Num partitions ... ${data.rdd.getNumPartitions}")

    val newData =
      data.transform(
        GenericTransformers
          .sanitise(epochCols, numericCols, boolCols, partitionCols)
      )

    val partition: String = appConfig.startDateOpt
      .map(IstTime.partitionFolder)
      .getOrElse(IstTime.partitionFolder(IstTime.now()))

    sink.put(appConfig, newData, Map(AppConfig.KeyNames.PARTITION -> partition))
  }
}
