package transforms

import org.apache.spark.sql.DataFrame

object GenericTransformers {

  def fixEpochs(timeCols: Set[String], df: DataFrame): DataFrame =
    timeCols.foldLeft(df)((newDf, col) => Udfs.toEpochSecond(col, newDf))
}
