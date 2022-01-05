// Databricks notebook source
//Ejercicio 1

val rdd = ???

val noSchemaDF = ???

display(noSchemaDF)



// COMMAND ----------

val df = ???
display(df)

// COMMAND ----------

import org.apache.spark.sql.types._

//Create DataFrame with schema
val myRdd = ???

//turn to rowsRDD
val rowsRDD = ???

//CreateSchema
val mySchema = ???

val peopleDF = ???


display(peopleDF)