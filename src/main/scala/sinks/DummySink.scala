package sinks
import config.AppConfig
import org.apache.spark.sql.DataFrame

object DummySink extends BaseSink {
  override def put(appConfig: AppConfig, data: DataFrame, params: Map[String, String]): Boolean = {
    // get owner spark session from DF itself
    //    val spark: SparkSession = data.sparkSession
    logger.info("Start Sink ...")
    data.printSchema()
    data.show(10000)
    true
  }
}
