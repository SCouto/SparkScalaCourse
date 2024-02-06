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

//Ejercicio 1, dia con cotizacion mas alta - Output

/*
highest price found for day:
2015-07-13 with value: 716.15 USD
2015-07-13 with value: 651.27 EUR
 */


val highestPriceRow = ???

// COMMAND ----------


//Ejercicio 2: media de precio de cierre - Output should be 230.52
val avgClosePrice = ???

// COMMAND ----------

//Ejercicio 3: maximo y minimo volumen
/*
+----------+---------+
|      date|   volume|
+----------+---------+
|2011-10-25|315541800|
  +----------+---------+

+----------+-------+
|      date| volume|
+----------+-------+
|2015-12-24|3531300|
+----------+-------+
  */

val volumeSorted = ???

// COMMAND ----------

//Ejercicio 4: dias con el precio de cierre mayor que 600 - Output should be 41
val daysAbove600 = ???

println(s"days when close was above 600 USD: $daysAbove600")


// COMMAND ----------

//Ejercicio 5: percentage of days when max price was above 500 - Output should be 4.92%
val totalDays = ???

val daysAbove500 = ???

println(s"percentage of days above 500 USD: ${(daysAbove500* 100.0f)/totalDays} %")

// COMMAND ----------

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

//Ejercicio 6: Maximo precio por ano  - Output
/*
+----+--------------+-----------+
|year|       highUSD|    highEUR|
+----+--------------+-----------+
|2011|        120.28|      87.02|
|2012|        133.42|     101.95|
|2013|        389.15|     284.90|
|2014|        489.29|     380.52|
|2015|        716.15|     651.27|
|2016|        129.28|     119.08|
+----+--------------+-----------+
 */
val maxPerYearWindowed = ???


val maxPerYearWindowedFx = ???

display(maxPerYearWindowedFx)


// COMMAND ----------

//Ejercicio 7: Precio medio de cierre por mes - Sample Output
/*
+-------+------+------+
|  month|avgUSD|avgEUR|
+-------+------+------+
|2011-10| 87.12| 62.31|
|2011-11| 79.76| 58.61|
|2011-12|  70.3| 53.36|
|2012-01| 97.75| 75.65|
|2012-02|119.92| 90.67|
|2012-03| 113.0| 85.51|
|2012-04|100.88| 76.59|
|2012-05| 72.99| 56.83|
|2012-06| 65.75| 52.42|
|2012-07| 75.25| 61.22|
|2012-08| 60.74|  49.0|
|2012-09| 56.58| 43.95|
|2012-10| 65.78| 50.71|
|2012-11| 80.04| 62.35|
|2012-12| 89.41| 68.17|
 */
val avgClosePerMonthWindowed = ???


val avgClosePerMonthWindowedFx = ???

display(avgClosePerMonthWindowedFx)