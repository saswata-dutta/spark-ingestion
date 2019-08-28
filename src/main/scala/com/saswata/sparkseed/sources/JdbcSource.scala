package com.saswata.sparkseed.sources

import java.util.Properties

import com.rivigo.vaayu.config.AppConfig
import com.rivigo.vaayu.sources.BaseSource.Credentials
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
    val properties = jdbcProps -- Seq(AppConfig.KeyNames.URL, AppConfig.KeyNames.DBTABLE)

    // it should create an instance of itself and register it with the DriverManager,
    // prevents java.sql.SQLException: No suitable driver
    // Class.forName("com.mysql.cj.jdbc.Driver")
    mysqlReader(spark, url, dbtable, BaseSource.credentials(appConfig), properties)
  }

  def mysqlReader(
    spark: SparkSession,
    url: String,
    dbtable: String,
    creds: Credentials,
    properties: Map[String, String]
  ): DataFrame = {

    val allProps = properties ++ Map(
        AppConfig.KeyNames.USER     -> creds.user,
        AppConfig.KeyNames.PASSWORD -> creds.password,
        "driver"                    -> "com.mysql.cj.jdbc.Driver"
      )

    val connectionProperties = new Properties()
    allProps.foreach { case (k, v) => connectionProperties.put(k, v) }

    spark.read
      .jdbc(url, dbtable, connectionProperties)
  }
}
