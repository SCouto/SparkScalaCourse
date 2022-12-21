// Databricks notebook source
// MAGIC %md 
// MAGIC 
// MAGIC 
// MAGIC 
// MAGIC ![Delta Lake](https://camo.githubusercontent.com/5535944a613e60c9be4d3a96e3d9bd34e5aba5cddc1aa6c6153123a958698289/68747470733a2f2f646f63732e64656c74612e696f2f6c61746573742f5f7374617469632f64656c74612d6c616b652d77686974652e706e67)
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
// MAGIC   - Obtén Lista los registros de la tabla `customer_data_delta_mini` para el cliente con id 20993
// MAGIC   - Debería salirte sólo 1 cliente

// COMMAND ----------

val clientDF = ???

display(clientDF)


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
// MAGIC     - Usa write.saveAsTable(customer_data_delta_to_upsert) para crear la tabla

// COMMAND ----------

import org.apache.spark.sql.functions.lit


val clientDF = ???

val upsertDF = ???

//upsertDF.write

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Ejecutamos el Merge, la sintaxis es :
// MAGIC 
// MAGIC - Merge into `<tabla destino>`
// MAGIC - using `<tabla origen>`
// MAGIC - on `clave de cruce`
// MAGIC - Gestiona cuando el registro coincide
// MAGIC   - Update
// MAGIC - Gestiona cuando no coincide
// MAGIC   - Insert

// COMMAND ----------


val sqlComand = """
  MERGE INTO <tabla_destino>
  USING <tabla con los registros a updatead>
  ON <tabla_destino>.<campo de cruce> = <tabla con los registros a updatead>.<campo de cruce>
  WHEN MATCHED THEN
    UPDATE SET
      <tabla_destino>.<Campo que debo actualizar> =  <tabla con los registros a updatead>.<Campo que debo actualizar>
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
// MAGIC - Comprueba el resultado cargando la tabla y filtrando por `customerId` 20993
// MAGIC    -  Usa nuevamente `spark.sql` o `spark.table`con `filter`

// COMMAND ----------
val clientDF = ???

display(clientDF)


// COMMAND ----------

// MAGIC %md 
// MAGIC 
// MAGIC # Sintaxis Scala
// MAGIC 
// MAGIC - Partimos de una tabla DeltaTable cargada 

// COMMAND ----------

import io.delta.tables.DeltaTable
val deltaTable = DeltaTable.forPath(miniDeltaDataPath)

// COMMAND ----------

// MAGIC %md
// MAGIC - Crea nuevamente un dataframe con el mismo cliente
// MAGIC - Usa `withColum` y `lit` para modificarlo y meter el valor fijo (usa 99998) en StockCode

// COMMAND ----------

val clientDF = ???

val upsertDF = ???

// COMMAND ----------

// MAGIC %md
// MAGIC - Ejecutamos el comando merge
// MAGIC 
// MAGIC    - deltaTable.merge(DatasetNuevo, clave de cruce) 
// MAGIC    - whenMatched (Indicar update con un Map)
// MAGIC    - whenNotMatched (insertAll)
// MAGIC    - execute

// COMMAND ----------

deltaTable.alias("t")
  	.merge(newUpsertDF.alias("u"),"t.<campo de cruce> = u.<campo de cruce>")
  	.whenMatched().updateExpr(Map("<campo a updatear>" -> "u.<campo a updatear>"))
  	.whenNotMatched().insertAll()
  	.execute()

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
