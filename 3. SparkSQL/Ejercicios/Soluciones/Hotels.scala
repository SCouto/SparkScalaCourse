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

//hoteles de Espanha
    val hotelsSpain = rawHotelsValid
      .where("cc1 = 'es'")
      .select("id", "cc1","name", "city_preferred")

display(hotelsSpain)

// COMMAND ----------

//hoteles de Espanha con ingresos
val hotelsSpainWithRevenues = hotelsSpain.join(rawRevenues,
      usingColumns = Seq("id"), joinType = "left")
      .select("name", "city_preferred", "revenue")

display(hotelsSpainWithRevenues)

// COMMAND ----------

//top 100  hoteles
val top100Hotels = rawHotelsValid.join(rawRevenues,
      usingColumns = Seq("id"), joinType = "left")
      .select("name", "city_preferred", "cc1", "revenue")
      .sort($"revenue".desc)
      .limit(100)

display(top100Hotels)

// COMMAND ----------

//top 200 ciudades
val top200cities = rawHotelsValid.join(rawRevenues,
      usingColumns = Seq("id"), joinType = "left")
      .select( "city_preferred", "revenue")
      .groupBy("city_preferred")
      .agg(sum($"revenue").alias("revenue"))
      .sort($"revenue".desc)
      .limit(200)

display(top200cities)


// COMMAND ----------

//sin ingresos
val noRevenues = rawHotelsValid.join(rawRevenues,
      usingColumns = Seq("id"),  joinType = "left")
      .where("revenue is null or revenue = 0")
      .select("name", "city_preferred", "cc1")

display(noRevenues)