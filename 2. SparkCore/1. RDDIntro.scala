// Databricks notebook source
//Ejercicio 1
val rdd = sc.parallelize(List(("Sergio", "1985-10-05"),("Ruth", "2000-05-01")))

// COMMAND ----------

rdd.collect().foreach(println)
rdd.count

// COMMAND ----------

// MAGIC %md 
// MAGIC **Ejercicio 2 Filter**: 
// MAGIC 
// MAGIC - Crea un RDD de números y filtra los números pares
// MAGIC   - Para saber si un número es par calcula su módulo 2 y compáralo con 0
// MAGIC   - número % 2  == 0

// COMMAND ----------

//Ejercicio 2 - Filter
val rddInts = ???

val rddFiltered = ???

display(rddFiltered.toDF)

// COMMAND ----------

// MAGIC %md
// MAGIC **Ejercicio3**
// MAGIC 
// MAGIC Sobre el mismo RDD anterior, convierte cada elemento en el doble de su valor

// COMMAND ----------

//Ejercicio 3 - Map
val rddMapped = ???

display(rddMapped.toDF)

// COMMAND ----------

// MAGIC %md 
// MAGIC 
// MAGIC **Ejercicio 4 flatMap**  
// MAGIC - Crea un RDD de Strings conde cada elemento sea una frase
// MAGIC   - Usa flatMap para splitearlo en palabras 
// MAGIC   - Para splitear un String por un espacio usa *_.split(" ")*
// MAGIC   
// MAGIC   
// MAGIC *Guarda este método porque lo usaremos en ejercicios posteriores*

// COMMAND ----------

//Ejercicio 4 - FlatMap
val rddSentences = ???

val rddWords = ???


display(rddWords.toDF)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC **Ejercicio 5 mapValues**
// MAGIC - Crea un PairRDD con la clave un nombre y el valor un número (sueldo)
// MAGIC   - Crea una lista de tuplas y paralelizala con *sc.parallelize*
// MAGIC     - *List(("elem1", "elem2"))*
// MAGIC   - Transfórmalo con mapValues para aumentar el sueldo en 500
// MAGIC   - Usa una lista de tuplas para crearlo

// COMMAND ----------

//Ejercicio 5 - mapValues
val rddEmpleados = ???

val rddEmpleadosUpdated = ???


display(rddEmpleadosUpdated.toDF)

// COMMAND ----------

// MAGIC %md
// MAGIC ** Ejercicio 6  reduceByKey**
// MAGIC 
// MAGIC - Crea un PairRDD usando una lista de tuplas donde:
// MAGIC   - El primer elemento sea el nombre de un departamento
// MAGIC   - El segundo elemento sea un número (cantidad de empleados p.e.)
// MAGIC - Usa reduceByKey para obtener el número total de empleados por departamento

// COMMAND ----------

//Ejercicio 6
val rddDepartamentos = ??

val rddDepartamentosUpdated = ??

display(rddDepartamentosUpdated.toDF)
