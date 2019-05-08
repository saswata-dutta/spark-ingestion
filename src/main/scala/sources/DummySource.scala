package sources
import config.AppConfig
import org.apache.spark.sql.{DataFrame, SparkSession}

object DummySource extends BaseSource {
  override def get(spark: SparkSession, config: AppConfig): DataFrame = {
    import spark.implicits._
    ((1 to 10000) map ((1550224873, true, _)))
      .toDF("created_at", "account_verified", "amount")
  }
}
