package com.saswata.sparkseed.transforms

import com.saswata.sparkseed.util.IstTime.YMDHM
import com.saswata.sparkseed.util.{IstTime, Strings}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, lit, udf, when}
import org.apache.spark.sql.types.DataType

import scala.util.{Failure, Success, Try}

@SuppressWarnings(Array("org.wartremover.warts.Any", "org.wartremover.contrib.warts.Apply"))
object Udfs {

  // functions for udfs, only for unit testing
  object Funcs {

    val strSanitiserFn: Any => String = (t: Any) =>
      Option(t).fold("null")(it => Strings.strictSanitise(it.toString))

    @SuppressWarnings(Array("org.wartremover.warts.Throw"))
    val toEpochMsFn: (String, Any) => Long = (col: String, e: Any) => {
      IstTime.guessEpochMillis(e) match {
        case Success(epoch) => epoch
        case Failure(ex) =>
          throw new IllegalArgumentException(s"Failed to convert Epoch in $col = $e", ex)
      }
    }

    val testDouble: Any => Boolean = (t: Any) => Try(t.toString.toDouble).isSuccess
    val testBool: Any => Boolean = (t: Any) => Try(t.toString.toBoolean).isSuccess

    @SuppressWarnings(Array("org.wartremover.contrib.warts.ExposedTuples"))
    val timePartitionFn: Long => (String, String, String) = (t: Long) => YMDHM(t).partitionValues
  }

  // udfs
  val toEpochMs: UserDefinedFunction =
    udf[Long, String, Any](Funcs.toEpochMsFn)

  val isDouble: UserDefinedFunction =
    udf[Boolean, Any](Funcs.testDouble)

  val isBool: UserDefinedFunction =
    udf[Boolean, Any](Funcs.testBool)

  val strSanitiser: UserDefinedFunction =
    udf[String, Any](Funcs.strSanitiserFn)

  val timePartition: UserDefinedFunction =
    udf[(String, String, String), Long](Funcs.timePartitionFn)

  // transforms
  def sanitiseText(colName: String)(df: DataFrame): DataFrame =
    df.withColumn(colName, strSanitiser(col(colName)))

  def toEpochMillis(colName: String)(df: DataFrame): DataFrame =
    df.withColumn(colName, toEpochMs(lit(colName), col(colName)))

  def castCol(colName: String, toType: DataType, isCompatible: UserDefinedFunction)(
    df: DataFrame
  ): DataFrame =
    df.withColumn(
      colName,
      when(col(colName).isNotNull && isCompatible(col(colName)), col(colName).cast(toType))
    )
}
