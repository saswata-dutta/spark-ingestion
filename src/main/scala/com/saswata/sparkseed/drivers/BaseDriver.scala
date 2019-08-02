package com.saswata.sparkseed.drivers

import com.saswata.sparkseed.config.AppConfig
import com.saswata.sparkseed.sinks.BaseSink
import com.saswata.sparkseed.sources.BaseSource
import com.saswata.sparkseed.util.{IstTime, Logging}
import com.saswata.sparkseed.{sinks, sources}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._

trait BaseDriver extends Logging {

  /*
    should be implemented by the concrete job
   */
  def run(spark: SparkSession, appConfig: AppConfig, source: BaseSource, sink: BaseSink): Boolean

  final def main(args: Array[String]): Unit = {
    val (sparkConf, config) = BaseDriver.loadConfig(args)
    val source = sources.BaseSource(config)
    val sink = sinks.BaseSink(config)

    val startTime = IstTime.now()
    logger.info("Started at " + startTime.toString)

    val spark = BaseDriver.createSpark(sparkConf)
    val status = run(spark, config, source, sink)
    spark.stop()

    val endTime = IstTime.now()
    logger.info("Finished at " + startTime.toString)
    logger.info(s"Time Taken ${endTime.toEpochSecond - startTime.toEpochSecond} seconds")
    logger.info(s"Job success ... $status")
  }
}

object BaseDriver extends Logging {

  def loadConfig(args: Array[String]): (SparkConf, AppConfig) = {
    val cmdArgs = parseArgs(args)
    logger.info("Cmd Args ...")
    cmdArgs.foreach(it => logger.info(it))
    val isLocal = cmdArgs.get(AppConfig.KeyNames.LOCAL).exists(_.toBoolean)

    val maybeConfName = cmdArgs.get(AppConfig.KeyNames.CONF_FILE)
    // loads application.conf or reference.conf
    val defaultConfig = ConfigFactory.load()
    val baseConfig: Config = {
      if (isLocal) ConfigFactory.load(AppConfig.KeyNames.LOCAL_CONF).withFallback(defaultConfig)
      else defaultConfig
    }
    val config: Config =
      maybeConfName
        .map(it => ConfigFactory.load(it).withFallback(baseConfig))
        .getOrElse(baseConfig)

    // for external file :
    // ConfigFactory.load(ConfigFactory.parseFile(file).resolve())
    val configKeyValues: Set[(String, String)] = config
      .entrySet()
      .asScala
      .map(e => (e.getKey, e.getValue.unwrapped().toString))
      .toSet

    val sparkValues =
      configKeyValues.filter(_._1.startsWith("spark.")).toMap

    val sparkConf =
      new SparkConf()
        .setAll(sparkValues)

    logger.info("Supplied Spark Conf ...")
    sparkConf.getAll.foreach(kv => logger.info(kv.toString()))

    logger.info("All Configs ...")
    configKeyValues
      .filterNot(_._1.startsWith("akka."))
      .toSeq
      .sortBy(_._1)
      .foreach(kv => logger.info(kv.toString()))

    val appConfig = new AppConfig(config, cmdArgs)

    (sparkConf, appConfig)
  }

  def parseArgs(args: Array[String]): Map[String, String] =
    args.grouped(2).filter(_.length == 2).map(it => (it(0), it(1))).toMap

  def createSpark(sparkConf: SparkConf): SparkSession = {
    val spark: SparkSession = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()

    logger.info("Spark Initialised with conf ...")
    spark.sparkContext.getConf.getAll.foreach { case (k, v) => logger.info(s"$k = $v") }

    spark
  }
}
