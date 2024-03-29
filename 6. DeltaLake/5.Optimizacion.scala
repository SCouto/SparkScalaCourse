// Databricks notebook source
// MAGIC %md 
// MAGIC 
// MAGIC 
// MAGIC ![Delta Lake](https://camo.githubusercontent.com/5535944a613e60c9be4d3a96e3d9bd34e5aba5cddc1aa6c6153123a958698289/68747470733a2f2f646f63732e64656c74612e696f2f6c61746573742f5f7374617469632f64656c74612d6c616b652d77686974652e706e67)
// MAGIC # Delta Lake 
// MAGIC 
// MAGIC 
// MAGIC ## Optimización
// MAGIC - Optimize
// MAGIC - Zorder

// COMMAND ----------

import org.apache.spark.sql.types._
import spark.implicits._
import org.apache.spark.sql.functions._

//Preparando entorno 
val inputPath = "dbfs:/FileStore/input/eventsStreaming"
val outputPath = "dbfs:/FileStore/output/iot"

dbutils.fs.rm(outputPath, true)

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
val rawDataDF = spark.read
                       .schema(mySchema)
                       .json(inputPath)
                       .select($"gt".alias("action"), ($"Creation_Time"/1E9).alias("time").cast("timestamp"))
                       .withColumn("date", to_date(from_unixtime($"time".cast("Long"),"yyyy-MM-dd")))
                       .withColumn("deviceId", expr("cast(rand(5) * 100 as int)"))
                       .repartition(1000)
                       .write
                       .mode("overwrite")
                       .format("delta")
                       .partitionBy("date")
                       .save(outputPath)




// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC # El fichero queda muy particionado debido al repartition
// MAGIC - Compruébalo usando el comando de abajo

// COMMAND ----------

dbutils.fs.ls(outputPath).foreach(d => dbutils.fs.ls(d.path).foreach(f =>println(f.path, s"${f.size/1024}Kb")))

// COMMAND ----------

// MAGIC %md
// MAGIC - Buscamos un ID y ejecutamos una query que sea altamente selectiva
// MAGIC   - Tada 1 minuto aprox

// COMMAND ----------

val devID = spark.sql(s"SELECT deviceId FROM delta.`$outputPath` limit 1").first()(0).asInstanceOf[Int]
val iotDF = spark.sql(s"SELECT * FROM delta.`$outputPath` where deviceId=$devID")
display(iotDF)

// COMMAND ----------

// MAGIC %md 
// MAGIC 
// MAGIC **Ejercicio**
// MAGIC - Optimiza el path haciendo zorder por deviceId y un optimize
// MAGIC - La Sentencia es:
// MAGIC - OPTIMIZE delta. \`< path >\` ZORDER by < columna >

// COMMAND ----------

// MAGIC %md 
// MAGIC 
// MAGIC - Comprueba la nueva estructura del directorio

// COMMAND ----------

dbutils.fs.ls(outputPath).foreach(d => dbutils.fs.ls(d.path).foreach(f =>println(f.path, s"${f.size/1024}Kb")))

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC - Ejecuta de nuevo la query altamente selectiva y compara el tiempo obtenido

// COMMAND ----------

val iotDF = spark.sql(s"SELECT * FROM delta.`$outputPath` where deviceId=$devID")
display(iotDF)

// COMMAND ----------

//Limpieza 
//dbutils.fs.rm(outputPath, true)
