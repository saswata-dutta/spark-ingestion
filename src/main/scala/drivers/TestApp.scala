package drivers

import config.AppConfig
import org.apache.spark.sql.SparkSession

object TestApp extends BaseDriver {

  override def run(spark: SparkSession, config: AppConfig): Unit = {
    val sc = spark.sparkContext
    val rdd = sc.parallelize(Seq(1, 2, 3))
    rdd.foreach(println)
  }
}
