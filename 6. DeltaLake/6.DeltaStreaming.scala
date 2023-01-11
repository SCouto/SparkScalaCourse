// Databricks notebook source
// MAGIC %md 
// MAGIC 
// MAGIC 
// MAGIC ![Delta Lake](https://camo.githubusercontent.com/5535944a613e60c9be4d3a96e3d9bd34e5aba5cddc1aa6c6153123a958698289/68747470733a2f2f646f63732e64656c74612e696f2f6c61746573742f5f7374617469632f64656c74612d6c616b652d77686974652e706e67)
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
// MAGIC # **Ejercicio 1** Stream tabla a tabla
// MAGIC
// MAGIC  Vamos a sacar un agregado por el tipo de actividad (ground truth)
// MAGIC - **Ejercicio**
// MAGIC   - Lee el stream generado en el paso anterior
// MAGIC   - Agrega por el campo `gt` y cuenta el número de elementos con count

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
// MAGIC - **Ejercicio**
// MAGIC   - Agrega en una ventana de 60 minutos sobre event time sobre el campo gt
// MAGIC   - Crea la ventana con window($"columna", "60 minutes")
// MAGIC   - Agrega con la siguiente sintaxis groupBy(window, columna) donde columna es gt
// MAGIC   - Contea los elementos en cada ventana con count

// COMMAND ----------

//Ejercicio 2 -  Gráficas
import org.apache.spark.sql.functions.{hour, window}

lazy val countsDF = deltaStreamWithTimestampDF      
  .withWatermark("event_time", "180 minutes")
//Agrega por la ventana  .groupBy(window($"event_time", "60 minutes"),$"gt")
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
