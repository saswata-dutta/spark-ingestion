package sinks
import config.AppConfig
import org.apache.spark.sql.DataFrame

object S3Sink extends BaseSink {
  override def put(appConfig: AppConfig, data: DataFrame, params: Map[String, String]): Boolean = {
    val sinkConfig = appConfig.conf.getConfig("sink")
    val bucket = sinkConfig.getString("bucket")
    val folder = sinkConfig.getString("folder")
    val partition = params.get("partition").map(p => s"$p/").getOrElse("")
    val outPath = s"s3a://$bucket/$folder/$partition"
    logger.info(s"Writing to $outPath ")

    val format = sinkConfig.getString("format")
    val saveMode = sinkConfig.getString("save_mode")
    data.coalesce(1).write.format(format).mode(saveMode).save(outPath)
    data.printSchema()
    data.show(100)
    true
  }
}
