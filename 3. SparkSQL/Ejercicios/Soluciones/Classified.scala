// Databricks notebook source
import org.apache.spark.sql.functions._


  val baseInputPath = "dbfs:/FileStore/input/classified"

  val sitesDF = spark.read.option("header", true).option("inferSchema", true).csv(s"$baseInputPath/sites.csv")
  val adsDF = spark.read.option("header", true).option("inferSchema", true).csv(s"$baseInputPath/ads.csv")
  val siteadsDF = spark.read.option("header", true).option("inferSchema",true).csv(s"$baseInputPath/siteads.csv").withColumn("publishedDate", to_date($"publishedDate", "yyyy-MM-dd'T'HH:mm:ss"))


// COMMAND ----------



  //Task 1: Localizar cuantos anuncios distintos se publican cada día
  val numberOfAdsByDayDF =  siteadsDF.select("publishedDate", "adId", "siteId").distinct.groupBy("publishedDate").count
  display(numberOfAdsByDayDF)

// COMMAND ----------

  //Task2 Calcular el beneficio medio diario de cada anuncio
  //Beneficio: price * impresions

  //Primero calcula impresiones por anuncio
  val impressionsByAdDF = siteadsDF.groupBy("adId", "publishedDate")
                                    .agg(sum("impressions").as("totalDailyImpressions"))

  //Cruzar con anuncios para obtener el precio
  val adsWithAvgRevenueDF = adsDF.join(impressionsByAdDF, Seq("adId"), "left")
    .select($"adName", $"publishedDate", ($"adPrice" * $"totalDailyImpressions").as("dailyRevenue"))
    .groupBy("adName")
    .agg(avg("dailyRevenue").as("avgDailyRevenue")).na.fill(0.0, Seq("avgDailyRevenue"))

display(adsWithAvgRevenueDF)



// COMMAND ----------

//Task3 Find out how many distinct ads have been published per siteName
  val adsBySiteIdDF = siteadsDF.groupBy("siteId").agg(countDistinct("adId").as("distinctAds"))

  val distinctAdsBySiteDF = sitesDF.join(adsBySiteIdDF, Seq("siteId"), "outer")
    .na.fill(0, Seq("distinctAds"))
    .withColumn("siteName", when($"siteName".isNull, $"siteId").otherwise($"siteName"))
    .select("siteName", "distinctAds")

display(distinctAdsBySiteDF)



// COMMAND ----------


  /*Task4 Crear una nueva columna la tabla anuncios llamada url. Para cada anuncio la url será:
Si el anuncio es par:
http://hash
 Si el anuncio es impar:
https://hash


  https://en.wikipedia.org/wiki/SHA-2 */

  /**
   * function to apply SHA-256
   * @param text Text to be cyphered
   * @return text in SHA-256
   */
  val sha256Hash = (text : String) => String.format("%064x", new java.math.BigInteger(1, java.security.MessageDigest.getInstance("SHA-256").digest(text.getBytes("UTF-8"))))

  /**
   * Same as above in UDF
   */
  val sha256HashUDF = udf(sha256Hash)


  val adsWithURLDF = adsDF.withColumn("url",
    when($"adId"%2 === 0,
      concat(lit(s"http://"), sha256HashUDF($"adName")))
      .otherwise(concat(lit(s"https://"), sha256HashUDF($"adName"))))

 display(adsWithURLDF)


