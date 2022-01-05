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
val prices = spark.read.option("inferSchema", true).option("header", true).csv(pricesPath).as[Price]

//Load fx rates
val fxRates = spark.read.option("inferSchema", true).option("header", true).csv(fxPath).as[FxRate].withColumn("date", from_unixtime(unix_timestamp($"date", "MMM dd, yyyy"), "yyyy-MM-dd"))

display(prices)

// COMMAND ----------

//Ejercicio 1, dia con cotizacion mas alta
val highestPriceRow = prices.select("date", "high").sort($"high".desc).take(1).head

val highestPriceDay = highestPriceRow.getAs[String]("date")
val highestPriceValue = highestPriceRow.getAs[Double]("high")
println(s"highest price found for day: $highestPriceDay with value: $highestPriceValue USD")

val fxRate = fxRates.where($"date" === highestPriceDay).select("High").head.getAs[Double](0)
println(s"highest price found for day: $highestPriceDay with value: ${highestPriceValue*fxRate} EUR")

// COMMAND ----------


//Ejercicio 2: media de precio de cierre
val avgClosePrice = prices.agg(avg("close")).take(1).head.getAs[Double](0)

// COMMAND ----------

//Ejercicio 3: maximo y minimo volumen
val volumeSorted = prices.select("date", "volume")
val highestVolumeRow = volumeSorted.sort($"volume".desc).take(1).head
val lowestVolumeRow = volumeSorted.sort($"volume".asc).take(1).head

val highestVolumeDay = highestVolumeRow.getAs[String]("date")
val highestVolumeValue = highestVolumeRow.getAs[Integer]("volume")

println(s"highest volume found for day: $highestVolumeDay with value: $highestVolumeValue")

val lowestVolumeDay = lowestVolumeRow.getAs[String]("date")
val lowestVolumeValue = lowestVolumeRow.getAs[Integer]("volume")
println(s"lowest volume found for day: $lowestVolumeDay with value: $lowestVolumeValue")

// COMMAND ----------

//Ejercicio 4: dias con el precio de cierre mayor que 600
val daysAbove600 = prices.filter($"close" > 600).select("date").distinct().count

println(s"days when close was above 600 USD: $daysAbove600")


// COMMAND ----------

//Ejercicio 5: percentage of days when max price was above 500
val totalDays = prices.select("date").distinct().count

val daysAbove500 = prices.filter($"high" > 500).select("date").distinct().count

println(s"percentage of days above 500 USD: ${(daysAbove500* 100.0f)/totalDays} %")

// COMMAND ----------

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

//Ejercicio 6: Maximo precio por ano
val maxPerYearWindowed = prices
      .withColumn("year", $"date".substr(0, 4))
      .withColumn("max_high",max($"high").over(Window.partitionBy("year")))
      .where($"high" === $"max_high")
      .select ("year", "high", "date")
      .withColumnRenamed("high", "highUSD")


val maxPerYearWindowedFx = maxPerYearWindowed.as("a")
      .join(fxRates.as("b"),  joinExprs = $"a.date" === $"b.date", joinType ="left")
        .select("a.year", "a.highUSD", "a.date", "b.High")
        .withColumn("highEUR", $"highUSD" * $"High")
        .select("year", "highUSD", "highEUR")

display(maxPerYearWindowedFx)


// COMMAND ----------

//Ejercicio 7: Precio medio de cierre por mes
val avgClosePerMonthWindowed = prices
      .withColumn("month", concat($"date".substr(0, 4), lit("-"), $"date".substr(6, 2)))
      .withColumn("max_close",max($"close").over(Window.partitionBy("month")))
      .where($"close" === $"max_close")
      .select ("month", "close", "date")
      .withColumnRenamed("close", "closeUSD")


val avgClosePerMonthWindowedFx = avgClosePerMonthWindowed.as("a")
      .join(fxRates.as("b"),  joinExprs = $"a.date" === $"b.date", joinType ="left")
      .select("a.month", "a.closeUSD", "a.date", "b.Open")
      .withColumn("closeEUR", $"closeUSD" * $"Open")
      .select("month", "closeUSD", "closeEUR")

display(avgClosePerMonthWindowedFx)