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
        val options =
          Seq(
            AppConfig.KeyNames.CSV_HEADER,
            AppConfig.KeyNames.CSV_INF_SCHEMA,
            AppConfig.KeyNames.CSV_DELIM,
            AppConfig.KeyNames.CSV_QUOTE,
            AppConfig.KeyNames.CSV_ESC,
            AppConfig.KeyNames.CSV_MODE
          ).map(it => it -> Try(sourceConfig.getString(it)).toOption)
            .collect { case (k, Some(v)) => (k, v) }
            .toMap

        csvReader(spark, options)

      case AppConfig.KeyNames.PARQUET =>
        spark.read
          .format(AppConfig.KeyNames.PARQUET)
    }

  @SuppressWarnings(Array("org.wartremover.warts.DefaultArguments"))
  def csvReader(
    spark: SparkSession,
    options: Map[String, String] = Map.empty[String, String]
  ): DataFrameReader = {
    val reader = spark.read
      .format(AppConfig.KeyNames.CSV)

    options.foldLeft(reader) {
      case (rdr, (k, v)) =>
        rdr.option(k, v)
    }
  }
}
