// Databricks notebook source
// MAGIC %md 
// MAGIC 
// MAGIC 
// MAGIC ![Delta Lake](https://camo.githubusercontent.com/5535944a613e60c9be4d3a96e3d9bd34e5aba5cddc1aa6c6153123a958698289/68747470733a2f2f646f63732e64656c74612e696f2f6c61746573742f5f7374617469632f64656c74612d6c616b652d77686974652e706e67)
// MAGIC # Delta Lake 
// MAGIC 
// MAGIC 
// MAGIC ## Añadir datos - Add

// COMMAND ----------

//Preparando entorno en parquet y delta

val baseInputPath = "dbfs:/FileStore/input/retail"
val inputPath = s"$baseInputPath/retail.csv"
val baseOuputPath = "dbfs:/FileStore/output/retail"
val parquetDataPath  = baseOuputPath + "/customer-data/"
val deltaDataPath    = baseOuputPath + "/customer-data-delta/"

spark.sql("DROP TABLE IF exists customer_data")
spark.sql("DROP TABLE IF exists customer_data_delta")
dbutils.fs.rm(baseOuputPath, true)


spark.read 
  .option("header", "true")
  .option("inferSchema", true)
  .csv(inputPath) 
  .write
  .mode("overwrite")
  .format("parquet")
  .partitionBy("Country")
  .save(parquetDataPath)


spark.sql(s"""
  CREATE TABLE IF NOT EXISTS customer_data 
  USING parquet 
  OPTIONS (path = "$parquetDataPath")
""")

spark.sql("MSCK REPAIR TABLE customer_data")

spark.table("customer_data").count

display(spark.table("customer_data").groupBy("Country").count.orderBy($"country"))


// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC # Añadiendo registros
// MAGIC 
// MAGIC - La tabla base tiene 14313 registros
// MAGIC   - Suecia: 41
// MAGIC   - España: 355
// MAGIC   - Sierra Leona no existe
// MAGIC - **Ejercicio**
// MAGIC   - Cargar el fichero retail_mini.csv que contiene 36 registros 
// MAGIC     - 18 registros de Suecia
// MAGIC     - 17 registros de Sierra Leona
// MAGIC     - 1 registro de España
// MAGIC   - Escríbelo en el mismo path
// MAGIC     - Recuerda usar append en vez de overwrite

// COMMAND ----------

//Carga el dataframe mini
val miniInputPath = s"$baseInputPath/retail_mini.csv"
val rawDF = ???

display(rawDF.groupBy("Country").count.orderBy($"country"))

// COMMAND ----------

//Escribimos en modo append en la ruta de parquet 
//rawDF.write


// COMMAND ----------

//Comprobamos registros (Deberían salir 14349)



// COMMAND ----------

// MAGIC %md
// MAGIC ### No actualiza registros
// MAGIC 
// MAGIC #### 1 partición nueva
// MAGIC  * Sierra Leona: No sabe que existe hasta que se actualizan metadatos
// MAGIC 
// MAGIC #### 2 particiones modificadas
// MAGIC  * Spain & Sweden: No es consciente de que ha modificado datos dentro de una partición ya existente
// MAGIC 
// MAGIC #### Tampoco es capaz de leer los datos nuevos
// MAGIC  - **Ejercicio** 
// MAGIC    - Ejecuta `display(spark.sql("select Country, count(*) from customer_data group by Country"))` y compruébalo
// MAGIC    - Luego ejecuta MSCK REPAIR TABLE y compruébalo de nuevo

// COMMAND ----------
val byCountryDF =

display(byCountryDF)

// COMMAND ----------

// MAGIC %sql
// MAGIC MSCK REPAIR TABLE customer_data

// COMMAND ----------

// MAGIC %md
// MAGIC ### Ahora lo mismo en Delta Lake
// MAGIC 
// MAGIC - Creamos la tabla en delta, es exactamente la misma sintasis, pero usando DELTA en lugar de PARQUET
// MAGIC - Llamándola customer_data_delta
// MAGIC - Ojo con el path
// MAGIC - **Ejercicio**
// MAGIC   - Cárgala usando `spark.table("<table>")` o `spark.sql("select from <table>")`
// MAGIC   - Haz un count y comprueba el resultado

// COMMAND ----------

//Preparación entorno
dbutils.fs.rm(deltaDataPath, true)
spark.sql("DROP TABLE IF exists customer_data_delta")

spark.read 
  .option("header", true)
  .option("inferSchema", true)
  .csv(inputPath) 
  .write
  .mode("overwrite")
  .format("delta")
  .partitionBy("Country")
  .save(deltaDataPath)

spark.sql(s"""
  CREATE TABLE IF NOT EXISTS customer_data_delta
  USING delta 
  OPTIONS (path = "$deltaDataPath")
""")


// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC - **Ejercicio**
// MAGIC   - Carga ahora el fichero retail_mini.csv que contiene 36 registros pero indicándole el schema que aparece en la celda
// MAGIC     - En vez de inferSchema, usa el método `schema(inputSchema)`
// MAGIC   - Escríbelo en la ruta de `deltaDataPat` en modo append

// COMMAND ----------

//Carga la el fichero retail_mini.csv de la misma forma que el dataset anterior y escríbelo en la ruta de delta en modo append
val miniInputPath = s"$baseInputPath/retail_mini.csv"
val inputSchema = "InvoiceNo STRING, StockCode STRING, Description STRING, Quantity INT, InvoiceDate STRING, UnitPrice DOUBLE, CustomerID String, Country STRING"

//Carga el dataframe mini
val rawDF = ???

val byCountryDF = ???

display(byCountryDF)


// COMMAND ----------

//Escribimos en modo append en la ruta de parquet 
//rawDF.write

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC - Comprueba que el resultado es correcto

// COMMAND ----------

display(byCountryDF)

// COMMAND ----------

//Limpieza
spark.sql("DROP TABLE IF exists customer_data")
spark.sql("DROP TABLE IF exists customer_data_delta")
dbutils.fs.rm(baseOuputPath, true)
