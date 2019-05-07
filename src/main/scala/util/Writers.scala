package util

import org.apache.spark.sql.{DataFrame, SaveMode}

trait Writers {

  def writeToCSV(df: DataFrame, folderName: String, saveMode: SaveMode = SaveMode.Overwrite) =
    df.repartition(1)
      .write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .mode(saveMode)
      .save(folderName)
}
