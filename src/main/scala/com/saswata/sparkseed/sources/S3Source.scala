package com.saswata.sparkseed.sources

import com.saswata.sparkseed.config.AppConfig
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession}

import scala.util.Try

object S3Source extends BaseSource {
  override def get(
    spark: SparkSession,
    appConfig: AppConfig,
    params: Map[String, String]
  ): DataFrame = {
    val sourceConfig = appConfig.sourceConfig
    val bucket = sourceConfig.getString(AppConfig.KeyNames.BUCKET)
    val folder = sourceConfig.getString(AppConfig.KeyNames.FOLDER)
    val suffix = appConfig.s3Suffix
    val s3Path = s"s3a://$bucket/$folder/$suffix"
    val format = sourceConfig.getString(AppConfig.KeyNames.FORMAT)

    logger.info("S3 Source Configs ...")
    logger.info(s"$s3Path|$format")

    val reader = getReader(spark, format, sourceConfig)
    reader.load(s3Path)
  }

  def getReader(spark: SparkSession, format: String, sourceConfig: Config): DataFrameReader =
    format match {
      case AppConfig.KeyNames.CSV =>
        spark.read
          .format(AppConfig.KeyNames.CSV)
          .option(
            "header",
            Try(sourceConfig.getBoolean(AppConfig.KeyNames.CSV_HEADER)).getOrElse(true)
          )
          .option(
            "inferSchema",
            Try(sourceConfig.getBoolean(AppConfig.KeyNames.CSV_INF_SCHEMA)).getOrElse(true)
          )
          .option(
            "delimiter",
            Try(sourceConfig.getString(AppConfig.KeyNames.CSV_DELIM)).getOrElse(",")
          )
          .option(
            "quote",
            Try(sourceConfig.getString(AppConfig.KeyNames.CSV_QUOTE)).getOrElse("\"")
          )
          .option("escape", Try(sourceConfig.getString(AppConfig.KeyNames.CSV_ESC)).getOrElse("\\"))
          .option("mode", "DROPMALFORMED")

      case AppConfig.KeyNames.PARQUET =>
        spark.read
          .format(AppConfig.KeyNames.PARQUET)
    }
}
