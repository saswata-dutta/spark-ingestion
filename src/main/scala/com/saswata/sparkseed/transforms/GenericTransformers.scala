package com.saswata.sparkseed.transforms

import com.saswata.sparkseed.util.Strings
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{BooleanType, DataType, DoubleType, StructType}
import org.apache.spark.sql.{Column, DataFrame}

import scala.util.Try

object GenericTransformers {

  def fixEpochs(timeCols: Set[String])(df: DataFrame): DataFrame =
    timeCols.foldLeft(df)((newDf, c) => newDf.transform(Udfs.toEpochMillis(c)))

  def sanitiseText(partitionCols: Set[String])(df: DataFrame): DataFrame =
    partitionCols.foldLeft(df)((newDf, c) => newDf.transform(Udfs.sanitiseText(c)))

  def castToType(cols: Set[String], toType: DataType, isCompatible: UserDefinedFunction)(
    df: DataFrame
  ): DataFrame =
    cols.foldLeft(df)((newDf, c) => newDf.transform(Udfs.castCol(c, toType, isCompatible)))

  def sanitise(
    timeCols: Set[String],
    numericCols: Set[String],
    boolCols: Set[String],
    partitionCols: Seq[String]
  )(df: DataFrame): DataFrame = {
    validateColsPresence(timeCols ++ numericCols ++ boolCols ++ partitionCols, df)

    df.transform(sanitiseText(partitionCols.toSet))
      .transform(fixEpochs(timeCols))
      .transform(castToType(boolCols, BooleanType, Udfs.isBool))
      .transform(castToType(numericCols, DoubleType, Udfs.isDouble))
  }

  def validateColsPresence(cols: Set[String], df: DataFrame): Unit = {
    val missingCols = cols.filterNot(hasCol(df))
    require(missingCols.isEmpty, s"Df is Missing cols $missingCols")
  }

  def hasCol(df: DataFrame)(col: String): Boolean = Try(df(col)).isSuccess

  def sanitiseColNames(df: DataFrame): DataFrame = {
    val sanitisedNames = df.columns.map(Strings.sanitise)
    require(
      sanitisedNames.length == sanitisedNames.toSet.size,
      s"Duplicate col names in DF after sanity ${sanitisedNames.mkString("|")}"
    )
    df.toDF(sanitisedNames: _*)
  }

  def dropNullCols(df: DataFrame): DataFrame =
    df.schema.fields
      .collect({ case x if x.dataType.typeName == "null" => x.name })
      .foldLeft(df)({ case (dframe, field) => dframe.drop(field) })

  def flattenSchema(schema: StructType, prefix: Option[String]): Array[Column] =
    schema.fields.flatMap(structField => {
      val colName = prefix.map(_ + ".").getOrElse("") + structField.name

      structField.dataType match {
        case st: StructType =>
          flattenSchema(st, Option(colName))
        case _ => Array(col(colName).alias(colName))
      }
    })

  def flatten(df: DataFrame): DataFrame =
    df.select(flattenSchema(df.schema, Option.empty[String]): _*)

  def addTimePartitions(ts: String)(df: DataFrame): DataFrame =
    df.withColumn("__time_part", Udfs.timePartition(col(ts)))
      .selectExpr("*", "__time_part._1 as y", "__time_part._2 as m", "__time_part._3 as d")
      .drop("__time_part")
}
