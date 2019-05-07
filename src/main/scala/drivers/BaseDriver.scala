package drivers

import com.typesafe.config.{Config, ConfigFactory}
import config.AppConfig
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import util.{IstTime, Logging}

import scala.collection.JavaConversions._

trait BaseDriver extends Logging {

  val appName: String = this.getClass.getName

  /*
    should be implemented by the concrete job
   */
  def run(spark: SparkSession, config: AppConfig): Unit

  def main(args: Array[String]): Unit = {
    val (sparkConf, config) = loadConfig(args.headOption)

    val startTime = IstTime.now()
    logger.info("Started at " + startTime.toString)
    val spark = createSpark(sparkConf)
    try {
      run(spark, config)
    } catch {
      case e: Exception =>
        logger.error("Failed to run Job ...", e)
    } finally {
      spark.stop()
      val endTime = IstTime.now()
      logger.info("Finished at " + startTime.toString)
      logger.info(
        s"Time Taken ${endTime.toEpochSecond - startTime.toEpochSecond} seconds"
      )
    }
  }

  private def loadConfig(
    maybeConfName: Option[String]
  ): (SparkConf, AppConfig) = {

    // load application.conf or reference.conf
    val baseConfig = ConfigFactory.load()
    val config: Config =
      maybeConfName
        .map(it => ConfigFactory.load(it).withFallback(baseConfig))
        .getOrElse(baseConfig)

    // for external file :
    // ConfigFactory.load(ConfigFactory.parseFile(file).resolve())

    val sparkValues = config
      .entrySet()
      .collect {
        case e if e.getKey.startsWith("spark") =>
          e.getKey -> e.getValue.unwrapped().toString
      }
      .toMap
    val sparkConf =
      new SparkConf()
        .setAppName(this.getClass.getSimpleName)
        .setAll(sparkValues)

    logger.info("All Configs ...")
    (sparkConf.getAll.toSet ++ config
      .entrySet()
      .map(e => e.getKey -> e.getValue.unwrapped().toString)
      .toSet)
      .filterNot(_._1.startsWith("akka."))
      .toSeq
      .sortBy(_._1)
      .foreach(kv => logger.info(kv.toString()))

    val appConfig = new AppConfig(config)

    (sparkConf, appConfig)
  }

  private def createSpark(sparkConf: SparkConf): SparkSession = {
    SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()
  }
}
