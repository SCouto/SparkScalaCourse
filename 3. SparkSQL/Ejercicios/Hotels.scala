// Databricks notebook source
// Databricks notebook source
val basePath =  "dbfs:/FileStore/input/hotels"
val baseOutputPath =  "dbfs:/FileStore/output/hotels"
val revenuesPath = s"$basePath/ingresos.csv"
val hotelsPath = s"$basePath/hoteleseuropa.csv"


case class Hotel (id: String , name: String, address: String, zip: String, city_hotel: String , cc1: String, ufi: String, hotel_class: String, currencycode: String, minrate: String, maxrate: String, preferred: String, nr_rooms: String, public_ranking: String, hotel_url: String, city_unique: String, city_preferred: String, review_score: String, review_nr: String)
case class HotelWrevenue (id: String , name: String, address: String, zip: String, city_hotel: String , cc1: String, ufi: String, hotel_class: String, currencycode: String, minrate: String, maxrate: String, preferred: String, nr_rooms: String, public_ranking: String, hotel_url: String, city_unique: String, city_preferred: String, review_score: String, review_nr: String, revenue: Double)
case class Revenue(id: String, revenue: Double)

// COMMAND ----------

import org.apache.spark.sql.functions._

//Load Revenues
val rawRevenues = spark
       .read
       .format("csv")
       .option("delimiter", ";")
       .option("header", "true")
       .load(revenuesPath)
       .withColumn("revenue", $"revenue".cast("double"))
       .groupBy($"id")
       .agg(sum("revenue").as("revenue"))
       .filter("revenue is not null")
       .as[Revenue]

//Load Hotels
val rawHotels = spark.read.format("csv")
      .option("delimiter", ";")
      .option("header", "true")
      .load(hotelsPath)
      .withColumnRenamed("class", "hotel_class")

val columnsToKeep = rawHotels.columns.filterNot(c => c.startsWith("_c")).map(a => rawHotels(a))

val rawHotelsValid = rawHotels
      .select(columnsToKeep:_*)
      .as[Hotel]

// COMMAND ----------

//hoteles de Espanha.  Output should be 428 hotels
    val hotelsSpain = ???

display(hotelsSpain)

// COMMAND ----------

//hoteles de Espanha cada uno con sus ingresos (incluso si sus ingresos son 0) - Output should be 428 hotels as well
val hotelsSpainWithRevenues = ???

display(hotelsSpainWithRevenues)

// COMMAND ----------

//top 100  hoteles - Top one is Las Olas in playas del ingles with 2720 revenue
val top100Hotels = ???

display(top100Hotels)

// COMMAND ----------

//top 200 ciudades - Top one is Malaga with 29470 revenue
val top200cities = ???

display(top200cities)

// COMMAND ----------

//sin ingresos - Output should be 4134 rows
val noRevenues = ???

display(noRevenues)