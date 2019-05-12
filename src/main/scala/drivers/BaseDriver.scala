package drivers

import com.typesafe.config.{Config, ConfigFactory}
import config.AppConfig
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sinks.BaseSink
import sources.BaseSource
import util.{IstTime, Logging}

import scala.collection.JavaConverters._

trait BaseDriver extends Logging {

  val appName: String = this.getClass.getName

  /*
    should be implemented by the concrete job
   */
  def run(spark: SparkSession, appConfig: AppConfig, source: BaseSource, sink: BaseSink): Boolean

  final def main(args: Array[String]): Unit = {
    val (sparkConf, config) = loadConfig(args)
    val source = BaseSource(config)
    val sink = BaseSink(config)

    val startTime = IstTime.now()
    logger.info("Started at " + startTime.toString)
    val spark = createSpark(sparkConf)
    try {
      val ok = run(spark, config, source, sink)
      logger.info(s"Job success ... $ok")
    } catch {
      case e: Exception =>
        logger.error("Failed to run Job ...", e)
    } finally {
      spark.stop()
      val endTime = IstTime.now()
      logger.info("Finished at " + startTime.toString)
      logger.info(s"Time Taken ${endTime.toEpochSecond - startTime.toEpochSecond} seconds")
    }
  }

  private def loadConfig(args: Array[String]): (SparkConf, AppConfig) = {
    val cmdArgs = parseArgs(args)
    // TODO local conf cmd flag and read flags for local running
    logger.info("Cmd Args ...")
    cmdArgs.foreach(it => logger.info(it))
    val isLocal = cmdArgs.get("--local").contains("true")

    val maybeConfName = cmdArgs.get("--conf-file")
    // loads application.conf or reference.conf
    val defaultConfig = ConfigFactory.load()
    val baseConfig =
      if (isLocal) ConfigFactory.load("local.conf").withFallback(defaultConfig) else defaultConfig
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
        .setAppName(this.getClass.getSimpleName)
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

  private def parseArgs(args: Array[String]): Map[String, String] =
    args.grouped(2).filter(_.length == 2).map(it => (it(0), it(1))).toMap

  private def createSpark(sparkConf: SparkConf): SparkSession = {
    val spark: SparkSession = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()

    logger.info("Spark Initialised with conf ...")
    spark.sparkContext.getConf.getAll.foreach { case (k, v) => logger.info(s"$k = $v") }

    spark
  }
}
