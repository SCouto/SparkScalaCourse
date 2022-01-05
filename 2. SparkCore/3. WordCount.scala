// Databricks notebook source
val inputPath = "dbfs:/FileStore/input/text/WarAndPeace.txt"
val outputPath = "dbfs:/FileStore/output/WordCount"


dbutils.fs.rm(outputPath, true)

// COMMAND ----------

//Ejercicio 1 WordCount
val linesRDD = sc.textFile(inputPath)


// COMMAND ----------

val wordsRDD = ???

//display(wordsRDD.toDF)

