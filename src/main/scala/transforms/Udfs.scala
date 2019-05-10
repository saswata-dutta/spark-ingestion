package transforms

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, udf}

object Udfs {

  def toEpochSecond(colName: String, df: DataFrame): DataFrame =
    df.withColumn(colName, toEpoch(col(colName)))

  private def guessEpoch(t: Any): Long = util.IstTime.guessEpochSecs(t).getOrElse(0L)
  private val toEpoch = udf(guessEpoch _)
}
