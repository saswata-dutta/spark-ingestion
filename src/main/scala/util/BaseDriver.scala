package util

import java.io.File

import com.typesafe.config.{Config, ConfigFactory}
import config.AppConfig
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.joda.time.DateTime

import scala.collection.JavaConversions._

trait BaseDriver extends Logging {
  val name: String = this.getClass.getName

  def main(args: Array[String]): Unit = {
    val (sparkConf, config) = loadConfig(args.headOption)
    lazy val spark = SparkSession.builder()
      .appName(name)
      .master(config.master)
      .config(sparkConf)
      .getOrCreate()

    val startTime = DateTime.now()
    logger.info("Started at " + startTime.toString)

    try{
      run(spark, config)
    } catch {
      case e: Exception =>
        logger.error("ERROR", e)

    } finally {
      val endTime = DateTime.now()
      logger.info("Finished at " + endTime.toString() + ", took " + ((endTime.getMillis.toDouble - startTime.getMillis.toDouble) / 1000))
      spark.stop()
    }
  }

  def run(session: SparkSession, config: AppConfig): Unit

  private def loadConfig(pathArg: Option[String]): (SparkConf, AppConfig) = {
    val config: Config = pathArg.map { path =>
      val file = new File(path)
      if(!file.exists()){
        throw new Exception("Config path " + file.getAbsolutePath + " doesn't exist!")
      }

      logger.info(s"Loading properties from " + file.getAbsolutePath)
      ConfigFactory.load(ConfigFactory.parseFile(file).resolve())
    }.getOrElse {
      ConfigFactory.load()
    }

    val sparkValues = config.entrySet().collect {
      case e if e.getKey.startsWith("spark") => e.getKey -> e.getValue.unwrapped().toString
    }.toMap
    val sparkConf = new SparkConf().setAppName(this.getClass.getName).setAll(sparkValues)

    logger.info("Config")
    (sparkConf.getAll.toSet ++ config.entrySet().map(e => e.getKey -> e.getValue.unwrapped().toString).toSet)
      .toList
      .filterNot(_._1.startsWith("akka."))
      .sortBy(_._1)
      .foreach(kv => logger.info(kv.toString()))

    sparkConf -> new AppConfig(config)
  }
}