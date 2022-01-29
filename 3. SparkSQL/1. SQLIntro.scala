// Databricks notebook source
// MAGIC %md
// MAGIC 
// MAGIC # Ejercicio 1
// MAGIC - Crea un RDD de tuplas de 3 elementos.
// MAGIC   - Contendrán nombre, edad  y sexo
// MAGIC - Conviértelo en DataFrame usando el método toDF y muestra el resultado usando df.show o display(df)

// COMMAND ----------

//Ejercicio 1

val rdd = ???

val noSchemaDF = ???

display(noSchemaDF)



// COMMAND ----------

// MAGIC %md 
// MAGIC # Ejercicio 2
// MAGIC 
// MAGIC - Convierte ahora el mismo RDD a dataframe indicándole el esquema
// MAGIC   - *rdd.toDF(“nombre”, “edad”, “sexo”)*
// MAGIC - Muestra el resultado

// COMMAND ----------

val df = ???
display(df)

// COMMAND ----------

// MAGIC %md 
// MAGIC 
// MAGIC # Ejercicio 3
// MAGIC 
// MAGIC - Usando los mismos datos del anterior ejercicio
// MAGIC 
// MAGIC - Convierte tu RDD en un RDD de Rows
// MAGIC   - *import org.apache.spark.sql.Row*
// MAGIC   - Mapea el RDD y conviértelo a rows extrayendo los valores como una tupla
// MAGIC   - *rdd.map(r => Row(r._1, r._2….))*
// MAGIC 
// MAGIC 
// MAGIC - Crea ahora un esquema StructType compuesto de los 3 StructField
// MAGIC   - StructType debe contener una Seq de 3 StructField
// MAGIC   - StructField debe contener nombre y tipo
// MAGIC   - StructField(“nombre”, StringType)
// MAGIC 
// MAGIC - Utiliza el método spark.createDataFrame para crear el DF pasándole el RDD de rows y el esquema

// COMMAND ----------

import org.apache.spark.sql.types._

//Create DataFrame with schema
val myRdd = ???

//turn to rowsRDD
val rowsRDD = ???

//CreateSchema
val mySchema = ???

val peopleDF = ???


display(peopleDF)

// COMMAND ----------

// MAGIC %md
// MAGIC # Ejercicio filter / select
// MAGIC 
// MAGIC - Partiendo del DataFrame dado, con las siguientes columnas: 
// MAGIC   - Nombre
// MAGIC   - Edad
// MAGIC 
// MAGIC - Filtra las personas que tengan más de 30 años
// MAGIC - Muestra por pantalla sus nombres

// COMMAND ----------

val df = List(("Sergio", 36),("Antía", 51), ("Pedro", 23),("Susana", 21)).toDF("nombre", "edad")




// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC # Ejercicio sort / create
// MAGIC 
// MAGIC - Partiendo del mismo dataframe que en el ejercicio anterior anterior
// MAGIC   - Ordena las personas por su edad de mayor a menor
// MAGIC   - Genera una columna con su año de nacimiento
// MAGIC       - Simplemente resta su edad al año actual
// MAGIC       - Para usar una constante usa el método *lit(valor)*
// MAGIC   - Renombra las columnas al inglés
// MAGIC   - Muéstralo por pantalla

// COMMAND ----------

import org.apache.spark.sql.functions.lit



