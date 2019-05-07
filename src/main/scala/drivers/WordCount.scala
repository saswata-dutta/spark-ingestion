package drivers

import config.AppConfig
import org.apache.spark.sql.SparkSession
import util.BaseDriver

object WordCount extends BaseDriver {

  override def run(session: SparkSession, config: AppConfig): Unit = {
    val sc = session.sparkContext
    val rdd = sc.parallelize(Seq(1,2,3))
    rdd.foreach(println)
  }
}