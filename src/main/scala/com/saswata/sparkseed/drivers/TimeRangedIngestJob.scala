package com.saswata.sparkseed.drivers

import com.saswata.sparkseed.config.AppConfig
import com.saswata.sparkseed.sinks.BaseSink
import com.saswata.sparkseed.sources.BaseSource
import com.saswata.sparkseed.transforms.GenericTransformers
import com.saswata.sparkseed.util.IstTime
import org.apache.spark.sql.{DataFrame, SparkSession}

/*
--class com.saswata.sparkseed.drivers.TimeRangedIngestJob
--conf-file trip_events.conf --start-epoch 1557340200 --stop-epoch 1557426600
--local true
 */
object TimeRangedIngestJob extends BaseDriver {
  override def run(
    spark: SparkSession,
    appConfig: AppConfig,
    source: BaseSource,
    sink: BaseSink
  ): Boolean = {

    val timeUnit = appConfig.timeUnit
    val startTime = appConfig.startTime
    val stopTime = appConfig.stopTime
    require(startTime < stopTime, s"$startTime is not before $stopTime")

    val timeCol = appConfig.timeCol
    val epochCols = appConfig.epochCols
    require(epochCols.contains(timeCol), s"$timeCol is not present in $epochCols")

    logger.info(s"Will fix epochs : $epochCols")
    val numericCols = appConfig.numericCols
    logger.info(s"Will cast to Double $numericCols")
    val boolCols = appConfig.boolCols
    logger.info(s"Will cast to Boolean $boolCols")
    val partitionCols = appConfig.partitionCols
    logger.info(s"Will sanitise text in  $partitionCols")

    val rawData: DataFrame = source.get(spark, appConfig)
    val sanitiser = GenericTransformers.sanitise(epochCols, numericCols, boolCols, partitionCols) _
    val ingester = runSlice(appConfig, sink, timeCol, rawData, sanitiser) _

    IstTime.dailyEpochRanges(startTime, stopTime, timeUnit).forall(ingester.tupled)
  }

  def runSlice(
    appConfig: AppConfig,
    sink: BaseSink,
    timeCol: String,
    rawData: DataFrame,
    sanitiser: DataFrame => DataFrame
  )(startTime: Long, stopTime: Long): Boolean = {

    logger.info(s"Fetching $startTime <= $timeCol < $stopTime ...")
    val newData =
      rawData
        .transform(sanitiser)
        .where(rawData(timeCol).geq(startTime) && rawData(timeCol).lt(stopTime)) // where is run before any udf

    val partition: String = IstTime.partitionFolder(startTime)
    sink.put(appConfig, newData, Map(AppConfig.KeyNames.PARTITION -> partition))
  }
}
