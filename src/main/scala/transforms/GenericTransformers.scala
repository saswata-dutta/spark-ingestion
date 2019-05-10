package transforms

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.DoubleType

object GenericTransformers {

  def fixEpochs(timeCols: Set[String], df: DataFrame): DataFrame =
    timeCols.foldLeft(df)((newDf, c) => Udfs.toEpochSecond(c, newDf))

  def castToDouble(numericCols: Set[String], df: DataFrame): DataFrame =
    numericCols.foldLeft(df)((newDf, c) => newDf.withColumn(c, col(c).cast(DoubleType)))

  def sanitise(timeCols: Set[String], numericCols: Set[String], df: DataFrame): DataFrame = {
    val fixedEpochs = fixEpochs(timeCols, df)
    castToDouble(numericCols, fixedEpochs)
  }
}
