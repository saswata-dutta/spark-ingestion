package sinks
import config.AppConfig
import org.apache.spark.sql.DataFrame

object DummySink extends BaseSink {
  override def put(config: AppConfig, data: DataFrame): Boolean = {
    // get owner spark session from DF itself
    //    val spark: SparkSession = data.sparkSession
    logger.info("Start Sink ...")
    data.printSchema()
    data.show(10000)
    true
  }
}
