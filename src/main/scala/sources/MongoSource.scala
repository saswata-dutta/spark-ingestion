package sources
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.ReadConfig
import config.AppConfig
import org.apache.spark.sql.{DataFrame, SparkSession}

object MongoSource extends BaseSource {
  override def get(
    spark: SparkSession,
    appConfig: AppConfig,
    params: Map[String, String]
  ): DataFrame = {
    val sourceConfig = appConfig.conf.getConfig("source")
    val mongoConfigs = Seq(("uri", "uri"), ("database", "database"), ("collection", "table")).map {
      case (k, v) => k -> sourceConfig.getString(v)
    }.toMap
    logger.info("Mongo Configs ...")
    logger.info(mongoConfigs)
    val readConfig = ReadConfig(mongoConfigs)

    MongoSpark.load(spark, readConfig)
  }
}
