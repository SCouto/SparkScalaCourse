// Databricks notebook source
// MAGIC %md
// MAGIC 
// MAGIC # Alturas
// MAGIC 
// MAGIC El objetivo es calcular la altura media por sexo
// MAGIC 
// MAGIC * Sube el fichero alturas.csv en Databricks, está explicado en el tutorial de Databricks  
// MAGIC * Comprueba el fichero, verás que es de la siguiente forma:
// MAGIC 
// MAGIC ```
// MAGIC H,178
// MAGIC M,179
// MAGIC ```
// MAGIC 
// MAGIC * Por defecto el método textFile carga cada registro en una línea
// MAGIC * El primer paso es dividir cada registro, utiliza el método map y un split por el separador (la coma)
// MAGIC * Ahora tienes en cada registro un array, haz un nuevo map que genere en cada registro una tupla, con los dos campos del array
// MAGIC   * Utiliza nombreQueLeDes(0) y nombreQueLeDes(1) para acceder a cada elemento de array
// MAGIC * Busca los que vengan en metros en lugar de en centímetros y convíertelos
// MAGIC   * Puedes buscar los que contentan un . 
// MAGIC * Convierte la altura a Double para poder operar con ella
// MAGIC * Filtra los datos erróneos (vacíos o negativos)
// MAGIC * Agrega por clave utilizando groupByKey
// MAGIC * Esto te devolverá un RDD de la siguiente forma:
// MAGIC ```(k, List(values))```
// MAGIC * Utiliza mapValues para trabajar con la lista y calcular la media
// MAGIC   * Puedes utilizar sum para sumar los elementos de la lista
// MAGIC   * Puedes utilizar size para calcular el número de elementos en la lista

// COMMAND ----------

//Ejercicio 2 Alturas
val linesRDD = sc.textFile("dbfs:/FileStore/input/alturas")


// COMMAND ----------

val resultsBySexRDD = ???

              
//display(resultsBySexRDD.toDF)
