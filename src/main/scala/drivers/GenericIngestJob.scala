package drivers

import config.AppConfig
import org.apache.spark.sql.{DataFrame, SparkSession}
import sinks.BaseSink
import sources.BaseSource
import transforms.Udfs

/*
--class drivers.GenericIngestJob --conf-file trip_events.conf --start-epoch 1557340200000000 --end-epoch 1557426600000000
 */
object GenericIngestJob extends BaseDriver {
  override def run(
    spark: SparkSession,
    appConfig: AppConfig,
    source: BaseSource,
    sink: BaseSink
  ): Boolean = {
    val startEpoch = appConfig.args.getOrElse("--start-epoch", "0")
    val endEpoch = appConfig.args.getOrElse("--end-epoch", "0")
    val timeCol = appConfig.conf.getConfig("schema").getString("time_partition_col")
    logger.info(s"Fetching $startEpoch <= $timeCol < $endEpoch ...")

    val data: DataFrame = source.get(spark, appConfig)
    val newData =
      Udfs
        .toEpochSecond(timeCol, data)
        .where(data(timeCol).geq(startEpoch) && data(timeCol).lt(endEpoch)) // where is run before any udf
    sink.put(appConfig, newData)
  }
}
