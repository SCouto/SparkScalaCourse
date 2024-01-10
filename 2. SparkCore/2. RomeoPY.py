# Databricks notebook source
dbutils.fs.rm("outputPath", True)
dbutils.fs.ls("dbfs:/FileStore/input/text/romeo.txt")

# COMMAND ----------

# MAGIC %md
# MAGIC ![Romeo](https://cdn.tatlerasia.com/asiatatler/i/hk/2020/01/24120707-romeo-juliet-zeffirelli-tatler0120_cover_1000x735.jpg)
# MAGIC
# MAGIC
# MAGIC 1. Sube el fichero romeo.txt en Databricks, está explicado en el tutorial de Databricks 
# MAGIC  
# MAGIC 2. Usa el método sc.textFile para cargar el fichero
# MAGIC   - La ruta será "dbfs:/FileStore/..."
# MAGIC
# MAGIC 3. Muestra el número de líneas usando el método count
# MAGIC
# MAGIC 4. Ejecuta el método collect sobre el RDD. ¿Qué devuelve?
# MAGIC
# MAGIC 5. Imprímelo por pantalla
# MAGIC   - Tendrás que usar un for
# MAGIC
# MAGIC ```
# MAGIC
# MAGIC     for element in rdd.collect():
# MAGIC
# MAGIC       print(element)
# MAGIC ``
# MAGIC
# MAGIC 6. Usa la función map para obtener la longitud de cada línea
# MAGIC   - Ejecuta de nuevo collect sobre el nuevo RDD e imprímelo de nuevo

# COMMAND ----------

romeo_rdd = sc.textFile("dbfs:/FileStore/input/text/romeo.txt")

# COMMAND ----------

#Count



# COMMAND ----------

#Collect


# COMMAND ----------

#Imprimir 




# COMMAND ----------

#Calcular tamaño e imprimir

