package com.saswata.sparkseed.sources

import com.saswata.sparkseed.config.AppConfig
import com.saswata.sparkseed.util.Logging
import com.saswata.sparkseed.util.aws.SsmClient
import org.apache.spark.sql.{DataFrame, SparkSession}

trait BaseSource extends Logging {

  @SuppressWarnings(Array("org.wartremover.warts.DefaultArguments"))
  def get(
    spark: SparkSession,
    config: AppConfig,
    params: Map[String, String] = Map.empty[String, String]
  ): DataFrame
}

object BaseSource {

  def apply(config: AppConfig): BaseSource = {
    val sourceType: String = sourceProp(config, AppConfig.KeyNames.TYPE)
    sourceType match {
      case AppConfig.KeyNames.DUMMY => DummySource
      case AppConfig.KeyNames.MONGO => MongoSource
      case AppConfig.KeyNames.JDBC  => JdbcSource
      case AppConfig.KeyNames.S3    => S3Source
    }
  }

  def sourceProps(config: AppConfig): Map[String, String] =
    AppConfig.configProps(config.sourceConfig)

  def sourceProp(config: AppConfig, key: String): String =
    AppConfig.configProp(config.sourceConfig, key)

  def dbProps(config: AppConfig): Map[String, String] =
    sourceProps(config) -- Seq(
      AppConfig.KeyNames.TYPE,
      AppConfig.KeyNames.SSM_PATH,
      AppConfig.KeyNames.AWS_REGION
    )

  final case class Credentials(user: String, password: String)

  def credentials(config: AppConfig): Credentials =
    if (config.noSsm) localCredentials(config) else ssmCredentials(config)

  def localCredentials(config: AppConfig): Credentials =
    Credentials(
      sourceProp(config, AppConfig.KeyNames.USER),
      sourceProp(config, AppConfig.KeyNames.PASSWORD)
    )

  def ssmCredentials(config: AppConfig): Credentials = {
    val ssmPath = sourceProp(config, AppConfig.KeyNames.SSM_PATH)
    val awsRegion = sourceProp(config, AppConfig.KeyNames.AWS_REGION)
    ssmCredentials(ssmPath, awsRegion)
  }

  def ssmCredentials(ssmPath: String, awsRegion: String): Credentials = {
    val props = SsmClient.getParamsByPath(ssmPath, awsRegion)
    Credentials(props(AppConfig.KeyNames.USER_NAME), props(AppConfig.KeyNames.PASSWORD))
  }
}
