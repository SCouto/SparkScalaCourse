// Databricks notebook source
// MAGIC %md 
// MAGIC # Structured Streaming
// MAGIC 
// MAGIC ## Eventos
// MAGIC 
// MAGIC ### Proporcionado 
// MAGIC - Sube los ficheros de eventos en formato json a la una carpeta nueva en la ruta que consideres
// MAGIC - Actualiza la variable eventsPath a la ruta donde subiste tus ficheros
// MAGIC - Carga el Stream indicando el esquema
// MAGIC 
// MAGIC ### A realizar
// MAGIC - Carga una ventana en base a la columna time, de 1 h de duración
// MAGIC   - Usa la siguiente sintasis val w = window(columna, duration)
// MAGIC   - La duración se indica en texto, por ejemplo: "1 hour"
// MAGIC   - Emplea esa ventana para agrupar por action y cuenta los valores. Es la misma sintaxis que con DataFrames
// MAGIC     - groupBy(columna, ventana) 
// MAGIC     - count
// MAGIC 
// MAGIC  - Escribe el resultado en memoria
// MAGIC    - usa writeStream con las siguientes opciones: 
// MAGIC      - format("memory")
// MAGIC      - outputMode("complete")
// MAGIC      - queryName(nombredeTabla) para poder hacer queries luego
// MAGIC    - Lanza start para que comience el procesamiento

// COMMAND ----------

import org.apache.spark.sql.types._
import spark.implicits._
import org.apache.spark.sql.functions._


val eventsPath = "dbfs:/FileStore/input/eventsStreaming"

 val mySchema = StructType(Seq(StructField("Arrival_Time", LongType), 
                StructField("Creation_Time", LongType),
                StructField("Device", StringType),
                StructField("Index", IntegerType),
                StructField("Model", StringType),
                StructField("User", StringType),
                StructField("gt", StringType),
                StructField("x", DoubleType),
                StructField("y", DoubleType),
                StructField("z", DoubleType)))

//Carga el Stream indicando el esquema 
val eventStream = spark.readStream.option("maxFilesPerTrigger", 1).schema(mySchema).json(eventsPath).select((col("Creation_Time")/1E9).alias("time").cast("timestamp"),col("gt").alias("action"))


// COMMAND ----------

display(eventStream)


// COMMAND ----------

display(eventStream)

// COMMAND ----------

//Crea una ventana sobre la columna time de 1h de duración
//Agrega por la columna action  sobre esa ventana
val w = ???
val myStream = ???

// COMMAND ----------

dbutils.fs.rm("dbfs:/local_disk0/tmp", true)
//myStream.writeStream.

// COMMAND ----------

// MAGIC %sql 
// MAGIC 
// MAGIC select action, date_format(window.start, "MMM-dd HH:mm") as timeStart, date_format(window.end, "MMM-dd HH:mm") as timeEnd, count 
// MAGIC from myTable 
// MAGIC order by timeStart, action

// COMMAND ----------

// MAGIC %fs ls "dbfs:/FileStore/input/eventsStreaming"
