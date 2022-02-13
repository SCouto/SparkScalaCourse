// Databricks notebook source
// MAGIC %md
// MAGIC 
// MAGIC # Input File 
// MAGIC 
// MAGIC - Events
// MAGIC - Schema
// MAGIC 
// MAGIC     - device:string
// MAGIC     - ecommerce:string
// MAGIC     - event_name:string
// MAGIC     - event_previous_timestamp:string
// MAGIC     - event_timestamp:long
// MAGIC     - geo:string
// MAGIC     - items:string
// MAGIC     - traffic_source:string
// MAGIC     - user_first_touch_timestamp:long
// MAGIC     - user_id:string

// COMMAND ----------

Class.forName("org.postgresql.Driver")


val eventsPath = "dbfs:/FileStore/input/events"

val df = spark.read.option("header", true).option("inferSchema", "true").option("quote", "\"").option("escape", "\"").csv(eventsPath)
display(df)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC # Query optimization
// MAGIC 
// MAGIC 1. Optimizaciones lógicas
// MAGIC 2. Predicate pushdown

// COMMAND ----------

// MAGIC %md 
// MAGIC 
// MAGIC ### Optimizaciones Lógicas
// MAGIC 
// MAGIC - Compara los planes lógicos y físicos de los siguientes dataframes

// COMMAND ----------


val filteredDF = df.filter($"event_name" =!= "reviews")
                 .filter($"event_name" =!= "checkout")
                 .filter($"event_name" =!= "register")
                 .filter($"event_name" =!= "email_coupon")
                 .filter($"event_name" =!= "cc_info")
                 .filter($"event_name" =!= "delivery")
                 .filter($"event_name" =!= "shipping_info")
                 .filter($"event_name" =!= "press")
                

filteredDF.explain(true)

// COMMAND ----------


val filteredDF = df.filter(
                 $"event_name" =!= "reviews" &&
                 $"event_name" =!= "checkout" &&
                 $"event_name" =!= "register" &&
                 $"event_name" =!= "email_coupon" &&
                 $"event_name" =!= "cc_info" &&
                 $"event_name" =!= "delivery" &&
                 $"event_name" =!= "shipping_info" &&
                 $"event_name" =!= "press")
                

filteredDF.explain(true)

// COMMAND ----------

val duplicatedFilteredDF = (df
            .filter($"event_name" =!= "finalize")
            .filter($"event_name" =!= "finalize")
            .filter($"event_name" =!= "finalize")
            .filter($"event_name" =!= "finalize")
            .filter($"event_name" =!= "finalize")
           )

duplicatedFilteredDF.explain(true)

// COMMAND ----------

// MAGIC %md 
// MAGIC 
// MAGIC ### Cacheos
// MAGIC 
// MAGIC - Tiene sentido si vas a usar el dataframe para varias cosas (Análisis exploratorio, entrenamiento de modelos)
// MAGIC 
// MAGIC <img src="https://files.training.databricks.com/images/icon_warn_32.png" alt="Warning"> Ojo al usarlo, cachear DataFrames Puede *degradar* el performance de la aplicación
// MAGIC 
// MAGIC - Los cacheos consumen recursos del cluster que no se podrán usar para otras tasks
// MAGIC - El cacheo puede provocar que que Spark no optimice las queries tanto como podría
// MAGIC 
// MAGIC #### Métodos
// MAGIC - San: Lectura tabla JDBC
// MAGIC - InMemoryScan: Lectura de tabla en memoria
// MAGIC 
// MAGIC #### Ejemplo 1
// MAGIC - No hay cacheo, se ve el pushedFilter en el plan físico
// MAGIC - Se ve método Scan con PushedFilter
// MAGIC - No se ve método InMemoryTableScan
// MAGIC 
// MAGIC 
// MAGIC 
// MAGIC #### Ejemplo 2
// MAGIC - Hay cacheo, no hay pushedFilter, traerá todo de memoria
// MAGIC - Se ve método Scan + InMemoryTableScan + Filter

// COMMAND ----------

val jdbcURL = "jdbc:postgresql://54.213.33.240/training"


val jdbcDF = spark.read
  .format("jdbc")
  .option("url", jdbcURL)
  .option("dbtable", "training.people_1m")
  .option("user", "training")
  .option("password", "training")
  .load()
  .filter($"gender" === "M")
       

jdbcDF.explain(true)

// COMMAND ----------


val jdbcURL = "jdbc:postgresql://54.213.33.240/training"


val jdbcDF = spark.read
  .format("jdbc")
  .option("url", jdbcURL)
  .option("dbtable", "training.people_1m")
  .option("user", "training")
  .option("password", "training")
  .load().cache


  val filteredDF = jdbcDF.filter($"gender" === "M")
       

filteredDF.explain(true)

// COMMAND ----------

// MAGIC %md 
// MAGIC # Partitioning
// MAGIC - Comprobando número de particiones
// MAGIC - Comprobar defaultParallelism para obtener los slots del cluster

// COMMAND ----------

df.rdd.getNumPartitions

// COMMAND ----------

println(spark.sparkContext.defaultParallelism)

// COMMAND ----------

val repartitonedDF = df.repartition(8)
repartitonedDF.rdd.getNumPartitions

// COMMAND ----------

val coalescedDF = repartitonedDF.coalesce(1)
repartitonedDF.rdd.getNumPartitions
