// Databricks notebook source
// MAGIC %md 
// MAGIC 
// MAGIC 
// MAGIC ![Delta Lake](https://camo.githubusercontent.com/5535944a613e60c9be4d3a96e3d9bd34e5aba5cddc1aa6c6153123a958698289/68747470733a2f2f646f63732e64656c74612e696f2f6c61746573742f5f7374617469632f64656c74612d6c616b652d77686974652e706e67)
// MAGIC # Delta Lake 
// MAGIC 
// MAGIC 
// MAGIC ## Time Travel

// COMMAND ----------

import org.apache.spark.sql.functions.lit

//Preparando entorno 
val baseInputPath = "dbfs:/FileStore/input/retail"
val miniInputPath = s"$baseInputPath/retail_mini.csv"
val baseOuputPath = "dbfs:/FileStore/output/retail"
val miniDeltaDataPath    = baseOuputPath + "/customer-mini-data-delta/"
spark.sql("DROP TABLE IF exists customer_data_delta_mini")
dbutils.fs.rm(baseOuputPath, true)

//Cargamos el fichero retail_mini.csv e la misma forma que el dataset anterior
spark.read 
  .option("header", "true")
  .option("inferSchema", true)
  .csv(miniInputPath) 
  .write
  .mode("overwrite")
  .format("delta")
  .save(miniDeltaDataPath) 
  
spark.sql(s"""
    CREATE TABLE IF NOT EXISTS customer_data_delta_mini
    USING DELTA 
    LOCATION "$miniDeltaDataPath" 
  """) 


import io.delta.tables.DeltaTable
val deltaTable = DeltaTable.forPath(miniDeltaDataPath)


val upsertDF = spark.sql("SELECT * FROM customer_data_delta_mini WHERE CustomerID=20993")
                    .withColumn("StockCode", lit(99999))
deltaTable.alias("t")
  	.merge(upsertDF.alias("u"),"t.CustomerId = u.CustomerID")
  	.whenMatched().updateExpr(Map("StockCode" -> "u.StockCode"))
  	.whenNotMatched().insertAll()
  	.execute()

deltaTable.delete($"CustomerId" === 20993)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC # Muestra la historia de la tabla 
// MAGIC - **Ejercicio**
// MAGIC   - Usa `deltaTable.history`
// MAGIC     - Devuelve un dataframe
// MAGIC     - Puedes mostrarlo con un display o un show

// COMMAND ----------

// MAGIC %md 
// MAGIC 
// MAGIC # Trabajando con versiones
// MAGIC 
// MAGIC - .toDF de delta table muestra última versión
// MAGIC - Si cargas el dataframe como delta => carga la última versión

// COMMAND ----------

display(deltaTable.toDF)

// COMMAND ----------

val df = spark.read.format("delta").load(miniDeltaDataPath)

display(df)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC - **Ejercicio**
// MAGIC  - Carga el dataframe como parquet y compara el contenido
// MAGIC  - Usa exactamente la misma sintaxis que en el comando anterior pero con format parquet

// COMMAND ----------

spark.sql("SET spark.databricks.delta.formatCheck.enabled=false")

// Cárgalo como parquet aquí, filtra por el clientId 20993 y muestra el resultado

// COMMAND ----------

// MAGIC %md 
// MAGIC 
// MAGIC # Trabajando con versiones antiguas
// MAGIC 
// MAGIC ##### Scala
// MAGIC - `spark.read.format("delta").option("versionAsOf", 0).load(path)`
// MAGIC 
// MAGIC ##### SQL
// MAGIC - `SELECT * FROM customer_data_delta_mini VERSION AS OF 3`
// MAGIC - `SELECT * FROM customer_data_delta_mini@202301100919420000000`

// COMMAND ----------

val df = spark.read.format("delta").option("versionAsOf", 0).load(miniDeltaDataPath)
display(df)

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC SELECT * FROM customer_data_delta_mini@202301100919420000000

// COMMAND ----------

// MAGIC %md
// MAGIC ![Delta Lake](https://c.tenor.com/qn_L3oU5rbIAAAAd/delorean-time-travel.gif)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC # Vacuum
// MAGIC 
// MAGIC - Es posible eliminar la historia de una tabla mediante vacuum
// MAGIC - Permite fijar un tiempo de retención
// MAGIC - Por defecto está protegido 

// COMMAND ----------

spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", false)

spark.sql("VACUUM customer_data_delta_mini RETAIN 0 HOURS ")

// COMMAND ----------

// MAGIC %md 
// MAGIC 
// MAGIC - Intenta leer ahora la versión 0

// COMMAND ----------

val df = spark.read.format("delta").option("versionAsOf", 0).load(miniDeltaDataPath)

display(df)

// COMMAND ----------

display(deltaTable.history)

// COMMAND ----------


//Limpieza 
//spark.sql("DROP TABLE IF exists customer_data_delta_mini")
//dbutils.fs.rm(baseOuputPath, true)
