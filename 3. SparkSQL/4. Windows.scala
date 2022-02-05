// Databricks notebook source
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

val data = Seq(
  (1.0, "Thin",       "cell phone", 6000),
  (1.0, "Normal",     "tablet",     1500),
  (2.0, "Mini",       "tablet",     5500),
  (3.0, "Ultra thin", "cell phone", 5000),
  (2.0, "Very thin",  "cell phone", 6000),
  (2.0, "Big",        "tablet",     2500),
  (2.0, "Bendable",   "cell phone", 3000),
  (2.0, "Foldable",   "cell phone", 3000),
  (3.0, "Pro",        "tablet",     4500),
  (4.0, "Pro2",       "tablet",     6500))
  .toDF("generation", "product", "category", "revenue")

display(data)

// COMMAND ----------

//Ejercicio 1 - Máximo por categoría
val overCategory = ???

val topSell = ???

display(topSell)

// COMMAND ----------

//Ejercicio 2 - top 2 ventas por categoría

val ranked = ???

display(ranked)

// COMMAND ----------

//Ejercicio 3 - Diferencia entre producto y top venta

val ranked = ???

display(ranked)