package com.saswata.sparkseed.drivers

import com.saswata.sparkseed.util.Logging

object SparkSanity extends Logging {

  def main(args: Array[String]): Unit = {
    val (sparkConf, _) = BaseDriver.loadConfig(args)
    val spark = BaseDriver.createSpark(sparkConf)
    spark.stop()
  }
}
