// Databricks notebook source
// MAGIC %md
// MAGIC ![Romeo](https://cdn.tatlerasia.com/asiatatler/i/hk/2020/01/24120707-romeo-juliet-zeffirelli-tatler0120_cover_1000x735.jpg)
// MAGIC 
// MAGIC 
// MAGIC 1. Sube el fichero romeo.txt en Databricks, está explicado en el tutorial de Databricks 
// MAGIC  
// MAGIC 2. Usa el método sc.textFile para cargar el fichero
// MAGIC   - La ruta será "dbfs:/FileStore/..."
// MAGIC 
// MAGIC 3. Muestra el número de líneas usando el método count
// MAGIC 
// MAGIC 4. Ejecuta el método collect sobre el RDD. ¿Qué devuelve?
// MAGIC 
// MAGIC 5. Imprímelo por pantalla
// MAGIC   - Usa el método foreach pasándole la función que quieres que aplique (println)
// MAGIC 
// MAGIC 6. Usa la función map para obtener la longitud de cada línea
// MAGIC   - Ejecuta de nuevo collect sobre el nuevo RDD e imprímelo de nuevo

// COMMAND ----------

val romeoRDD = sc.textFile("dbfs:/FileStore/input/text/romeo.txt")

// COMMAND ----------

//Count


// COMMAND ----------

//Collect


// COMMAND ----------

//Imprimir 


// COMMAND ----------

//Calcular tamaño e imprimir

