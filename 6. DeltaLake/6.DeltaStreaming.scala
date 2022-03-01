// Databricks notebook source
// MAGIC %md 
// MAGIC 
// MAGIC 
// MAGIC ![Delta Lake](https://live-delta-io.pantheonsite.io/wp-content/uploads/2019/04/delta-lake-logo-tm.png)
// MAGIC # Delta Lake 
// MAGIC 
// MAGIC 
// MAGIC ## Streaming

// COMMAND ----------

val dataPath = "dbfs:/FileStore/input/eventsStreaming"

//Preparando rutas
val baseOutputPath = "dbfs:/FileStore/output/streaming"
dbutils.fs.rm(baseOutputPath, true)

// COMMAND ----------

// MAGIC %md 
// MAGIC 
// MAGIC Cargamos el Stream de entrada

// COMMAND ----------

//Carga el Stream de entrada
lazy val static = spark.read.json(dataPath)
lazy val dataSchema = static.schema

val deltaStreamWithTimestampDF = spark
  .readStream
  .option("maxFilesPerTrigger", 1)
  .schema(dataSchema)
  .json(dataPath)
  .withColumnRenamed("Index", "User_ID")
  .selectExpr("*","cast(cast(Arrival_Time as double)/1000 as timestamp) as event_time")

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Escribiendo el Stream

// COMMAND ----------

val writePath =  baseOutputPath  + "activity/data"
val checkpointPath = baseOutputPath + "activity/checkpoint"

//Escribe el Stream en formato delta
val deltaStreamingQuery = deltaStreamWithTimestampDF
  .writeStream
  .format("delta")
  .option("checkpointLocation", checkpointPath)
  .outputMode("append")
  .queryName("myActivityStream")
  .start(writePath)

// COMMAND ----------

for (stream <- spark.streams.active) { 
  println(s"${stream.name}: ${stream.id}")
}

// COMMAND ----------

display(spark.sql(s"select * from delta.`$writePath`"))

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC # ** Ejercicio 1** Stream tabla a tabla
// MAGIC 
// MAGIC - **Ejercicio**
// MAGIC   - En la lectura del stream generado en el paso anterior
// MAGIC   - Agrega por el campo `gt` y cuenta el número de elementos

// COMMAND ----------

//Ejercicio 1 - Stream tabla a tabla
val activityPath =  baseOutputPath  + "count/data"
val checkpointPath = baseOutputPath + "count/checkpoint"

val activityCountsQuery = spark.readStream
  .format("delta")
  .load(writePath)
//Aquí añade la agregación y el conteo
  .writeStream
  .format("delta")
  .option("checkpointLocation", checkpointPath)
  .outputMode("complete")
  .queryName("myActivityCountStream")
  .start(activityPath)

// COMMAND ----------

display(spark.sql(s"select * from delta.`$activityPath`"))

// COMMAND ----------

// MAGIC %md 
// MAGIC 
// MAGIC #Ejercicio 2
// MAGIC 
// MAGIC ## Gráficas
// MAGIC - ** Ejercicio**
// MAGIC   - Añade el count sobre el dataframe para realizar el conteo

// COMMAND ----------

//Ejercicio 2 -  Gráficas
import org.apache.spark.sql.functions.{hour, window}

lazy val countsDF = deltaStreamWithTimestampDF      
  .withWatermark("event_time", "180 minutes")
  .groupBy(window($"event_time", "60 minute"),$"gt")
//Aquí añade el count



// COMMAND ----------

// MAGIC %md
// MAGIC #Ejercicio 3
// MAGIC 
// MAGIC - Ejecuta el display y haz la gráfica
// MAGIC   - Entra en Plot options
// MAGIC   - Selecciona el tipo de gráfica 
// MAGIC   - Selecciona la serie de agregación(gt) y los valores (count)

// COMMAND ----------

display(countsDF.withColumn("hour",hour($"window.start")), streamName = "AnotherStreamName")

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC select * from AnotherStreamName

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Parando los streams

// COMMAND ----------

for (stream <- spark.streams.active) { 
  println(s"${stream.name}: ${stream.id}")
  stream.stop()
}

// COMMAND ----------

//Limpieza
dbutils.fs.rm(baseOutputPath, true)
