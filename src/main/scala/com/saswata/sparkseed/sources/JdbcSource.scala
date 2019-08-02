package com.saswata.sparkseed.sources

import java.util.Properties

import com.saswata.sparkseed.config.AppConfig
import org.apache.spark.sql.{DataFrame, SparkSession}

object JdbcSource extends BaseSource {
  override def get(
    spark: SparkSession,
    appConfig: AppConfig,
    params: Map[String, String]
  ): DataFrame = {
    val jdbcProps: Map[String, String] = BaseSource.dbProps(appConfig)
    logger.info("Jdbc Configs ...")
    logger.info(jdbcProps)

    val url = jdbcProps(AppConfig.KeyNames.URL)
    val dbtable = jdbcProps(AppConfig.KeyNames.DBTABLE)
    val connectionProperties = new Properties()
    (jdbcProps -- Seq(AppConfig.KeyNames.URL, AppConfig.KeyNames.DBTABLE) ++ credentials(appConfig))
      .foreach(e => connectionProperties.put(e._1, e._2))

    // it should create an instance of itself and register it with the DriverManager,
    // prevents java.sql.SQLException: No suitable driver
    // Class.forName("com.mysql.cj.jdbc.Driver")
    val _ = connectionProperties.put("driver", "com.mysql.cj.jdbc.Driver")

    spark.read
      .jdbc(url, dbtable, connectionProperties)
  }

  def credentials(appConfig: AppConfig): Map[String, String] = {
    val creds = BaseSource.credentials(appConfig)
    Map(AppConfig.KeyNames.USER -> creds.user, AppConfig.KeyNames.PASSWORD -> creds.password)
  }
}
