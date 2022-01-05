// Databricks notebook source
val inputPath = "dbfs:/FileStore/input/alturas/alturas.csv"
val outputPath = "dbfs:/FileStore/output/alturas"


dbutils.fs.rm(outputPath, true)

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


 val mToCMudf = ???

    import spark.implicits._

    val avgDF  = ???

display(avgDF)