// Databricks notebook source
// MAGIC %md 
// MAGIC 
// MAGIC 
// MAGIC ![Delta Lake](https://live-delta-io.pantheonsite.io/wp-content/uploads/2019/04/delta-lake-logo-tm.png)
// MAGIC # Delta Lake 
// MAGIC 
// MAGIC 
// MAGIC ## Upsert And Delete

// COMMAND ----------

//Preparando entorno en parquet y delta

val baseInputPath = "dbfs:/FileStore/input/retail"
val miniInputPath = s"$baseInputPath/retail_mini.csv"
val baseOuputPath = "dbfs:/FileStore/output/retail"
val miniDeltaDataPath    = baseOuputPath + "/customer-mini-data-delta/"
spark.sql("DROP TABLE IF exists customer_data_delta_mini")
spark.sql("DROP TABLE IF EXISTS customer_data_delta_to_upsert")

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

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### Sin Delta Lake
// MAGIC 
// MAGIC - No se puede, habría que cargar el dataframe entero, hacer el join y operar en consonancia
// MAGIC - Si es un dataset muy grande => pain

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### Con Delta Lake
// MAGIC 
// MAGIC - Sintaxis propia y optimizada
// MAGIC - **Ejercicio** 
// MAGIC   - Lista los registros para cliente con id 20993
// MAGIC   - Debería salirte sólo 1 cliente

// COMMAND ----------

// MAGIC %md 
// MAGIC 
// MAGIC # Sintaxis SQL
// MAGIC 
// MAGIC 
// MAGIC 
// MAGIC - **Ejercicio**
// MAGIC   - Objetivo: Crear una tabla llamada ```customer_data_delta_to_upsert``` con únicamente ese registro pero el `StockCode = 99999`
// MAGIC   - Pasos: 
// MAGIC     - Crea un dataframe con únicamente ese registro cargando la tabla customer_data_delta_mini con un `filter/where`
// MAGIC     - Usa `withColum` y `lit` para meter el valor fijo
// MAGIC     - Usa write.saveAsTable para crear la tabla

// COMMAND ----------

import org.apache.spark.sql.functions.lit


// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Ejecutamos el Merge

// COMMAND ----------


val sqlComand = """
  MERGE INTO customer_data_delta_mini
  USING customer_data_delta_to_upsert
  ON customer_data_delta_mini.CustomerID = customer_data_delta_to_upsert.CustomerID
  WHEN MATCHED THEN
    UPDATE SET
      customer_data_delta_mini.StockCode = customer_data_delta_to_upsert.StockCode
  WHEN NOT MATCHED
    THEN INSERT (InvoiceNo, StockCode, Description, Quantity, InvoiceDate, UnitPrice, CustomerID, Country)
    VALUES (
      customer_data_delta_to_upsert.InvoiceNo,
      customer_data_delta_to_upsert.StockCode, 
      customer_data_delta_to_upsert.Description, 
      customer_data_delta_to_upsert.Quantity, 
      customer_data_delta_to_upsert.InvoiceDate, 
      customer_data_delta_to_upsert.UnitPrice, 
      customer_data_delta_to_upsert.CustomerID, 
      customer_data_delta_to_upsert.Country)"""
spark.sql(sqlComand)

// COMMAND ----------

// MAGIC %md
// MAGIC ** Ejercicio**
// MAGIC - Comprueba el resultado cargando la tabla y filtrando por `customerId` 20993
// MAGIC    -  Usa nuevamente `spark.sql` o `spark.table`con `filter`

// COMMAND ----------

// MAGIC %md 
// MAGIC 
// MAGIC # Sintaxis Scala
// MAGIC 
// MAGIC - Partimos de una tabla DeltaTable cargada 
// MAGIC - **Ejercicio**
// MAGIC   - Crea nuevamente un dataframe con el mismo cliente y un valor diferente en StockCode
// MAGIC   - Usa `withColum` y `lit` para meter el valor fijo
// MAGIC   - Ejecutamos el comando merge

// COMMAND ----------

import io.delta.tables.DeltaTable
val deltaTable = DeltaTable.forPath(miniDeltaDataPath)

// COMMAND ----------

// MAGIC %md 
// MAGIC 
// MAGIC Comprueba el resultado con
// MAGIC 
// MAGIC `display(spark.sql(s"SELECT * FROM delta.`$miniDeltaDataPath` WHERE CustomerID=20993"))`
// MAGIC `display(spark.sql("SELECT * FROM customer_data_delta_mini WHERE CustomerID=20993"))`

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC # Delete
// MAGIC 
// MAGIC **Ejercicio**
// MAGIC - Borra el cliente que acabamos de modificar 
// MAGIC - Igual que antes tenemos varias opciones, usa la sintaxis que más te guste
// MAGIC 
// MAGIC 
// MAGIC ##### SQL
// MAGIC - `DELETE FROM <database>.<table> WHERE CustomerID=20993` Estamos usando default como database, por eso puede obviarse
// MAGIC - `DELETE FROM delta.<path> WHERE CustomerID=20993` Path debe ir entre acentos graves
// MAGIC 
// MAGIC ##### Scala
// MAGIC 
// MAGIC - `deltaTable.delete($"CustomerId" === 20993)`

// COMMAND ----------

// MAGIC %md 
// MAGIC 
// MAGIC Comprueba los ficheros de metadatos que se crearon

// COMMAND ----------

// MAGIC %fs ls "dbfs:/FileStore/output/retail/customer-mini-data-delta/"

// COMMAND ----------


//Limpieza 
//spark.sql("DROP TABLE IF exists customer_data_delta_mini")
//spark.sql("DROP TABLE IF EXISTS customer_data_delta_to_upsert")
//dbutils.fs.rm(baseOuputPath, true)
