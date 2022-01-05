// Databricks notebook source
val inputPath = "dbfs:/FileStore/input/alturas/alturas.csv"
val outputPath = "dbfs:/FileStore/output/alturas"


dbutils.fs.rm(outputPath, true)

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


 val mToCMudf = udf((s: String) => {
    if (s.contains("."))
      s.toDouble * 100
    else s.toDouble
  })


    val rawDF = spark
      .read
      .csv(inputPath)
      .toDF(List("sexo", "altura"): _*)
      .createOrReplaceTempView("tmpTable")

    val avgDF = ???


display(avgDF)