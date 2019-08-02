package com.saswata.sparkseed.sinks

import com.saswata.sparkseed.config.AppConfig
import org.apache.spark.sql.DataFrame

object S3Sink extends BaseSink {
  override def put(appConfig: AppConfig, data: DataFrame, params: Map[String, String]): Boolean = {
    val sinkConfig = appConfig.sinkConfig
    val bucket = sinkConfig.getString(AppConfig.KeyNames.BUCKET)
    val folder = sinkConfig.getString(AppConfig.KeyNames.FOLDER)
    val partition = params.get(AppConfig.KeyNames.PARTITION).map(p => s"$p/").getOrElse("")
    val outPath = s"s3a://$bucket/$folder/$partition"
    logger.info(s"Writing to $outPath ")

    val processed = BaseSink.process(appConfig)(data)
    val format = sinkConfig.getString(AppConfig.KeyNames.FORMAT)
    val saveMode = sinkConfig.getString(AppConfig.KeyNames.SAVE_MODE)
    processed.coalesce(1).write.format(format).mode(saveMode).save(outPath)

    true
  }
}
