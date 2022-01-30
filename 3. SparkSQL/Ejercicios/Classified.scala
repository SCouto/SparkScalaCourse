// Databricks notebook source
import org.apache.spark.sql.functions._


  val baseInputPath = "dbfs:/FileStore/input/classified"

  val sitesDF = spark.read.option("header", true).option("inferSchema", true).csv(s"$baseInputPath/sites.csv")
  val adsDF = spark.read.option("header", true).option("inferSchema", true).csv(s"$baseInputPath/ads.csv")
  val siteadsDF = spark.read.option("header", true).option("inferSchema",true).csv(s"$baseInputPath/siteads.csv").withColumn("publishedDate", to_date($"publishedDate", "yyyy-MM-dd'T'HH:mm:ss"))


// COMMAND ----------



  //Task 1: Localizar cuantos anuncios distintos se publican cada día
  val numberOfAdsByDayDF =  ???

  display(numberOfAdsByDayDF)

// COMMAND ----------

  //Task2 Calcular el beneficio medio diario de cada anuncio
  //Beneficio: price * impresions

  //Primero calcula impresiones por anuncio
  val impressionsByAdDF = ???

  //Cruzar con anuncios para obtener el precio
  val adsWithAvgRevenueDF = ???

display(adsWithAvgRevenueDF)



// COMMAND ----------

//Task3 Cuántos anuncios distintos se publicaron por site (mostrando el nombre del site)
  val adsBySiteIdDF = ???

  val distinctAdsBySiteDF = ???

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
   * Same as above but ready to use as UDF
   */
  val sha256HashUDF = udf(sha256Hash)


  val adsWithURLDF = ???

 display(adsWithURLDF)


