package com.saswata.sparkseed.config

import com.saswata.sparkseed.util.IstTime
import com.typesafe.config.{Config, ConfigFactory}

import scala.collection.JavaConverters._

/**
  * Inspired by
  * https://github.com/typesafehub/config/blob/master/examples/scala/simple-lib/src/main/scala/simplelib/SimpleLib.scala
  *
  * Fail fast when dealing with config values!
  */
final class AppConfig(conf: Config, args: Map[String, String]) {

  //use default config on classpath if no config is passed here
  def this() {
    this(ConfigFactory.load(), Map.empty[String, String])
  }

  val isLocal: Boolean = args.get(AppConfig.KeyNames.LOCAL).exists(_.toBoolean)
  val noSsm: Boolean = args.get(AppConfig.KeyNames.NO_SSM).exists(_.toBoolean)
  val startDateOpt: Option[String] = args.get(AppConfig.KeyNames.START_DATE)
  val s3Suffix: String = args.getOrElse(AppConfig.KeyNames.S3_SUFFIX, "")

  val schemaConfig: Config = conf.getConfig(AppConfig.KeyNames.SCHEMA)
  val sourceConfig: Config = conf.getConfig(AppConfig.KeyNames.SOURCE)
  val sinkConfig: Config = conf.getConfig(AppConfig.KeyNames.SINK)

  val timeCol: String = schemaConfig.getString(AppConfig.KeyNames.TIME_COL)
  val timeUnit: String = schemaConfig.getString(AppConfig.KeyNames.TIME_COL_U)
  val startTime: Long = convertTimeArg("start", timeUnit)
  val stopTime: Long = convertTimeArg("stop", timeUnit)
  private def convertTimeArg(prefix: String, unit: String): Long =
    args
      .get(s"--$prefix-epoch")
      .filter(_.length == 10)
      .map(_.toLong)
      .orElse(args.get(s"--$prefix-date").map(d => IstTime.dateStrToEpochSecs(d)))
      .map(e => IstTime.scaleEpochSecs(e, unit))
      .getOrElse(0L)

  val flattenCols: Boolean = schemaConfig.getBoolean(AppConfig.KeyNames.FLATTEN)

  val epochCols: Set[String] =
    schemaConfig.getStringList(AppConfig.KeyNames.EPOCH_COLS).asScala.toSet

  val numericCols: Set[String] =
    schemaConfig.getStringList(AppConfig.KeyNames.NUMERIC_COLS).asScala.toSet

  val boolCols: Set[String] =
    schemaConfig.getStringList(AppConfig.KeyNames.BOOL_COLS).asScala.toSet

  val partitionCols: Seq[String] = {
    val cols = schemaConfig.getStringList(AppConfig.KeyNames.PARTITION_COLS).asScala.toVector
    require(
      cols.size == cols.toSet.size,
      s"Duplicate col names in in Partition Cols ${cols.mkString("|")}"
    )
    cols
  }
}

object AppConfig {

  def configProps(subConfig: Config): Map[String, String] = {
    val keys = subConfig.entrySet().asScala.map(_.getKey).toSet
    keys.foldLeft(Map.empty[String, String])((acc, key) => acc + (key -> subConfig.getString(key)))
  }

  def configProp(subConfig: Config, key: String): String =
    subConfig.getString(key)

  object KeyNames {
    val SCHEMA: String = "schema"
    val SOURCE: String = "source"
    val SINK: String = "sink"

    val LOCAL: String = "--local"
    val START_DATE: String = "--start-date"
    val STOP_DATE: String = "--stop-date"
    val START_EPOCH: String = "--start-epoch"
    val STOP_EPOCH: String = "--stop-epoch"
    val CONF_FILE: String = "--conf-file"
    val LOCAL_CONF: String = "local.conf"
    val S3_SUFFIX: String = "--s3-suffix"
    val NO_SSM: String = "--no-ssm"

    val EPOCH_COLS: String = "epoch_cols"
    val NUMERIC_COLS: String = "numeric_cols"
    val BOOL_COLS: String = "bool_cols"
    val PARTITION_COLS: String = "partition_cols"
    val TIME_COL: String = "time_partition_col"
    val TIME_COL_U: String = "time_partition_col_unit"
    val FLATTEN: String = "flatten"

    val TYPE: String = "type"
    val DUMMY: String = "dummy"
    val S3: String = "s3"
    val S3_TIME_PART: String = "s3_time_part"
    val MONGO: String = "mongo"
    val JDBC: String = "jdbc"

    val USER: String = "user"
    val USER_NAME: String = "username"
    val PASSWORD: String = "password"
    val SSM_PATH: String = "ssm-path"
    val AWS_REGION: String = "aws-region"
    val URL: String = "url"
    val URI: String = "uri"
    val HOST: String = "host"
    val DBTABLE: String = "dbtable"

    val BUCKET: String = "bucket"
    val FOLDER: String = "folder"
    val PARTITION: String = "partition"
    val FORMAT: String = "format"
    val SAVE_MODE: String = "save_mode"

    val CSV = "csv"
    val CSV_HEADER = "header"
    val CSV_DELIM = "delimiter"
    val CSV_QUOTE = "quote"
    val CSV_ESC = "escape"
    val CSV_INF_SCHEMA = "inferSchema"
    val CSV_MODE = "mode"

    val PARQUET = "parquet"
  }
}
