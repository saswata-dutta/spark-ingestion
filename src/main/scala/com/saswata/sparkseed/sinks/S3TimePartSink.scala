package com.saswata.sparkseed.sinks

import com.saswata.sparkseed.config.AppConfig
import com.saswata.sparkseed.transforms.GenericTransformers
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SaveMode}

object S3TimePartSink extends BaseSink {
  override def put(appConfig: AppConfig, data: DataFrame, params: Map[String, String]): Boolean = {
    val sinkConfig = appConfig.sinkConfig
    val bucket = sinkConfig.getString(AppConfig.KeyNames.BUCKET)
    val folder = sinkConfig.getString(AppConfig.KeyNames.FOLDER)
    val outPath = s"s3a://$bucket/$folder/"
    logger.info(s"Writing to $outPath ...")

    val timeCol = appConfig.timeCol

    val processed =
      data
        .transform(BaseSink.process(appConfig))
        .transform(GenericTransformers.addTimePartitions(timeCol))

    val partitionCols = appConfig.partitionCols.map(col)
    val partitionFolders = appConfig.partitionCols ++ Seq("y", "m", "d")
    val format = sinkConfig.getString(AppConfig.KeyNames.FORMAT)

    val outDf = processed.repartition(partitionCols: _*).sortWithinPartitions(col(timeCol))
    outDf.write
      .partitionBy(partitionFolders: _*)
      .format(format)
      .mode(SaveMode.Append)
      .save(outPath)

    true
  }

}
