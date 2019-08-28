package com.saswata.sparkseed.sources

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.ReadConfig
import com.saswata.sparkseed.config.AppConfig
import org.apache.spark.sql.{DataFrame, SparkSession}

object MongoSource extends BaseSource {
  override def get(
    spark: SparkSession,
    appConfig: AppConfig,
    params: Map[String, String]
  ): DataFrame = {
    val mongoProps: Map[String, String] = connectionProps(appConfig)
    logger.info("Mongo Configs ...")
    logger.info(mongoProps)
    val readConfig = ReadConfig(mongoProps)

    appConfig.explicitSchema.fold(MongoSpark.loadAndInferSchema(spark, readConfig))(
      schema =>
        MongoSpark
          .builder()
          .sparkSession(spark)
          .readConfig(readConfig)
          .build()
          .toDF(schema)
    )
  }

  def connectionProps(appConfig: AppConfig): Map[String, String] = {
    val props: Map[String, String] = BaseSource.dbProps(appConfig)
    val host = props(AppConfig.KeyNames.HOST)
    val creds = BaseSource.credentials(appConfig)
    val uri = s"mongodb://${creds.user}:${creds.password}@$host/admin"
    (props + (AppConfig.KeyNames.URI -> uri)) -- Seq(
      AppConfig.KeyNames.HOST,
      AppConfig.KeyNames.USER,
      AppConfig.KeyNames.PASSWORD
    )
  }
}
