// Databricks notebook source
// MAGIC %md 
// MAGIC 
// MAGIC # File Stream
// MAGIC 
// MAGIC Ejercicio para contar palabras en ficheros de texto sobre un un directorio
// MAGIC 
// MAGIC - Genera un StreamingContext con 5 segundos de ventana
// MAGIC - Emplea textFileStream para "escuchar" un directorio (inputPath)
// MAGIC - Trata el Stream generado para realizar un wordcount, es la misma sintaxis que con RDD
// MAGIC   - Splitealo por espacios usando flatMap
// MAGIC   - Reemplaza los caracteres extraños con _.replaceAll("\\W+", "").toLowerCase() dentro de un map
// MAGIC   - Filtra las palabras vacías
// MAGIC   - Mapea cada palabra a una tupla de la siguiente forna: (palabra, 1)
// MAGIC   - Agrupa por clave con groupByKey
// MAGIC   - Reduce por clave sumando los valores
// MAGIC 
// MAGIC - Muestra el resultado por pantalla con print
// MAGIC   - Alternativa: foreachRDD, ordenar, convertir a DF y emplearshow
// MAGIC - En la misma celda donde haces print, arranca el streaming context con ssc.start y ssc.awaitTermination()
// MAGIC 
// MAGIC ### Pruebas
// MAGIC - Mueve un fichero al directorio de entrada
// MAGIC - Comprueba la salida
// MAGIC 
// MAGIC ### Pausa
// MAGIC 
// MAGIC - Para pararlo cancela la ejecución
// MAGIC - Si quieres probar otra vez espera 2-3 minutos
// MAGIC 
// MAGIC ### Notas: Tras cada prueba borra el directorio de entrada con dbutils.fs.rm

// COMMAND ----------

import org.apache.spark.sql.types._
import spark.implicits._
import org.apache.spark.sql.functions._
import org.apache.spark.streaming._

val inputPath = "dbfs:/FileStore/input/streaming/text"
dbutils.fs.rm(inputPath, true)


// COMMAND ----------

val ssc = new StreamingContext(sc, Seconds(5))
val textDS = ssc.textFileStream(inputPath)

// COMMAND ----------

val wordCountDS = ???

// COMMAND ----------


wordCountDS.print

ssc.start
ssc.awaitTermination()



// COMMAND ----------

dbutils.fs.rm(inputPath, true)
ssc.stop()
