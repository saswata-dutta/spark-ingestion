package sources
import config.AppConfig
import org.apache.spark.sql.{DataFrame, SparkSession}

object DummySource extends BaseSource {
  override def get(
    spark: SparkSession,
    appConfig: AppConfig,
    params: Map[String, String]
  ): DataFrame = {
    import spark.implicits._
    logger.info("Start Source ...")
    (1 to 10000)
      .map((1550224873, true, _))
      .toDF("created_at", "account_verified", "amount")
  }
}
