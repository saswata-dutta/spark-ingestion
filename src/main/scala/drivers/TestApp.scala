package drivers

import config.AppConfig
import org.apache.spark.sql.SparkSession

object TestApp extends BaseDriver {

  override def run(session: SparkSession, config: AppConfig): Unit = {
    val sc = session.sparkContext
    val rdd = sc.parallelize(Seq(1, 2, 3))
    rdd.foreach(println)
  }
}
