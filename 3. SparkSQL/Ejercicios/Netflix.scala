// Databricks notebook source
val basePath =  "dbfs:/FileStore/input/netflix"
val baseOutputPath =  "dbfs:/FileStore/output/netflix"
val pricesPath = s"$basePath/Netflix_2011_2016.csv"
val fxPath = s"$basePath/USD_EUR_Historical_Data.csv"


case class FxRate (date: String, price: Double, open: Double, high: Double, low: Double, change: String)
case class Price (date: String,open: Double, high: Double, low: Double, close: Double, volume: Integer, adjclose: Double)


// COMMAND ----------

import org.apache.spark.sql.functions._


//Load prices
val prices = ???

//Load fx rates
val fxRates = spark.read.option("inferSchema", true).option("header", true).csv(fxPath).as[FxRate].withColumn("date", from_unixtime(unix_timestamp($"date", "MMM dd, yyyy"), "yyyy-MM-dd"))

display(prices)

// COMMAND ----------

//Ejercicio 1, dia con cotizacion mas alta
val highestPriceRow = ???

// COMMAND ----------


//Ejercicio 2: media de precio de cierre
val avgClosePrice = ???

// COMMAND ----------

//Ejercicio 3: maximo y minimo volumen
val volumeSorted = ???

// COMMAND ----------

//Ejercicio 4: dias con el precio de cierre mayor que 600
val daysAbove600 = ???

println(s"days when close was above 600 USD: $daysAbove600")


// COMMAND ----------

//Ejercicio 5: percentage of days when max price was above 500
val totalDays = ???

val daysAbove500 = ???

println(s"percentage of days above 500 USD: ${(daysAbove500* 100.0f)/totalDays} %")

// COMMAND ----------

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

//Ejercicio 6: Maximo precio por ano
val maxPerYearWindowed = ???


val maxPerYearWindowedFx = ???

display(maxPerYearWindowedFx)


// COMMAND ----------

//Ejercicio 7: Precio medio de cierre por mes
val avgClosePerMonthWindowed = ???


val avgClosePerMonthWindowedFx = ???

display(avgClosePerMonthWindowedFx)