// Databricks notebook source
// MAGIC %md 
// MAGIC 
// MAGIC 
// MAGIC ![Delta Lake](https://live-delta-io.pantheonsite.io/wp-content/uploads/2019/04/delta-lake-logo-tm.png)
// MAGIC # Delta Lake 
// MAGIC 
// MAGIC 
// MAGIC ## Introducción - Creación de tablas

// COMMAND ----------

val inputPath = "dbfs:/FileStore/input/retail/retail.csv"
val ouputPath = "dbfs:/FileStore/output/retail"
val parquetDataPath  = ouputPath + "/customer-data/"
val deltaDataPath    = ouputPath + "/customer-data-delta/"

spark.sql("DROP TABLE IF exists customer_data")
spark.sql("DROP TABLE IF exists customer_data_delta")
dbutils.fs.rm(ouputPath, true)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### Cargando el dataframe
// MAGIC 
// MAGIC - **Ejercicio**: Carga el dataframe usando spark.read
// MAGIC   - Usa el `option` `header` con `true` para coger la cabecera
// MAGIC   - Usa la `option` `inferSchema` para inferir el esquema
// MAGIC   - Usa el método `csv`para cargar el Dataframe

// COMMAND ----------

//Carga el dataframe y comprueba cuál es el pais que menos transacciones tiene
val rawDF = ???

display(rawDF.groupBy("Country").count.orderBy($"country"))

// COMMAND ----------

// MAGIC %md 
// MAGIC 
// MAGIC ### Escribiendo el dataframe
// MAGIC 
// MAGIC 
// MAGIC - **Ejercicio** Escribe la tabla en parquet y en delta
// MAGIC   - Usa el método `write`
// MAGIC   - Usa el método `mode("overwrite")`
// MAGIC   - Usa el método `format("parquet")` y en otro comando `format("delta")`
// MAGIC   - Usa el método `partitionBy("Country"`
// MAGIC   - Por último usa el método `save` indicando el path correcto (`parquetDataPath` o `deltaDataPat``)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Creación de tablas en parquet
// MAGIC 
// MAGIC - Crea la tabla en parquet llamándola customer_data
// MAGIC - Cárgala usando `spark.table("<table>")` o `spark.sql("select from <table>")`
// MAGIC - **Ejercicio**
// MAGIC   - Haz un count y comprueba el resultado

// COMMAND ----------

//Crea una tabla en parquet
spark.sql(s"""
  CREATE TABLE IF NOT EXISTS customer_data 
  USING parquet 
  OPTIONS (path = "$parquetDataPath")
""")

//Carga la tabla aquí y haz un count


// COMMAND ----------

// MAGIC %md
// MAGIC ### Por qué  0 registros? 
// MAGIC 
// MAGIC De acuerdo con el concepto de Datalake que vimos, schema-on-read, el schema se aplica al leer, en lugar del al almacenar
// MAGIC 
// MAGIC  * Los datos están en el path **`parquetDataPath`** en el sistema de ficheros
// MAGIC  * Los datos correspondientes a metadatos, están en otro sitio (Metastore)
// MAGIC  * Al añadir datos a un fichero, no se actualizan los metadatos, necesitamos correr un comando extra
// MAGIC  
// MAGIC  
// MAGIC **`MSCK REPAIR TABLE <tableName>`**
// MAGIC 
// MAGIC * **Ejercicio**
// MAGIC   * Ejecuta el comando MSCK Repair table
// MAGIC   * Comprueba el resultado del count ahora

// COMMAND ----------

// MAGIC %md
// MAGIC ### Creación de tablas en Delta
// MAGIC 
// MAGIC - **Ejercicio**
// MAGIC   - Crea la tabla en delta, es exactamente la misma sintasis, pero usando DELTA en lugar de PARQUET
// MAGIC   - Llámala customer_data_delta
// MAGIC   - Ojo con el path
// MAGIC   - Cárgala usando `spark.table("<table>")` o `spark.sql("select from <table>")`
// MAGIC   - Haz un count y comprueba el resultado

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC 
// MAGIC 
// MAGIC Ya no hay problema con 0 registros, ni necesidad de actualiar el metastore
// MAGIC 
// MAGIC 
// MAGIC ![Delta Lake](https://c.tenor.com/4lMPHnN8oeQAAAAM/guy-long-hair.gif)

// COMMAND ----------

// MAGIC %fs ls "dbfs:/FileStore/output/retail/customer-data-delta/_delta_log"
