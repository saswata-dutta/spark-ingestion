package sources
import config.AppConfig
import org.apache.spark.sql.{DataFrame, SparkSession}

object DummySource extends BaseSource {
  override def get(spark: SparkSession, config: AppConfig): DataFrame = {
    import spark.implicits._
    Seq((1550224873, true, 1000.0))
      .toDF("created_at", "account_verified", "amount")
  }
}
