// Databricks notebook source


// COMMAND ----------

// MAGIC %md 
// MAGIC 
// MAGIC # Cargando datos en RDD y DataFrame
// MAGIC 
// MAGIC - Cargarás un dataset con la forma
// MAGIC 
// MAGIC usuario password
// MAGIC 
// MAGIC - Donde el objetivo es:
// MAGIC  - Calcular contraseñas que son vacías
// MAGIC  - Calcular contraseñas que son iguales al nombre de usuario
// MAGIC  - Calcular el resto

// COMMAND ----------

val basePath =  "dbfs:/FileStore/input/passwords"
val passwordsSamplePath = s"$basePath/plain_passwords_sample.txt"
val passwordsPath = s"$basePath/plain_passwords_half.txt"
val path = passwordsPath


import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val rdd = spark.sparkContext.textFile(path)
      .map(line => {
        line.split("\t", -1) match {
          case Array(user, password) => (user, password)
          case Array(user) => (user, "")
        }
      })


val df = rdd.toDF("username", "password")

// COMMAND ----------

// MAGIC %md 
// MAGIC # RDD: Usando filters y count
// MAGIC 
// MAGIC - Utiliza filter para contar las contraseñas vacías
// MAGIC - Utiliza filter y count para contar las contraseñas que sean iguales al usuario
// MAGIC - Cuenta el resto de passwords

// COMMAND ----------

val initTime = System.nanoTime


val countEmpty = ???
val countEqual = ???
val countUnknown = ???


println(
  s"""Empty passwords:        $countEmpty
    |Password equal to user: $countEqual
    |Unknown passwords:      $countUnknown
  """.stripMargin)

val durationRDD = (System.nanoTime - initTime) / 1e9d

print(s"duration in seconds: $durationRDD")

// COMMAND ----------

// MAGIC %md 
// MAGIC # DataFrame: Usando filters/where y count
// MAGIC 
// MAGIC - Utiliza filter para contar las contraseñas vacías
// MAGIC - Utiliza filter y count para contar las contraseñas que sean iguales al usuario
// MAGIC - Cuenta el resto de passwords

// COMMAND ----------

val initTime = System.nanoTime


val countEmpty = ???
val countEqual = ???
val countUnknown = ???


println(
  s"""Empty passwords:        $countEmpty
    |Password equal to user: $countEqual
    |Unknown passwords:      $countUnknown
  """.stripMargin)

val durationDF = (System.nanoTime - initTime) / 1e9d

print(s"duration in seconds: $durationDF")

// COMMAND ----------

// MAGIC %md 
// MAGIC # RDD: Mejorando el tiempo con cache
// MAGIC 
// MAGIC - si haces 3 veces el count, tiene sentido pensar que el cache te mejorará los tiempos, pruébalo

// COMMAND ----------

val initTime = System.nanoTime

val rddCached = rdd.cache

val countEmpty = ???
val countEqual = ???
val countUnknown = ???


println(
  s"""Empty passwords:        $countEmpty
    |Password equal to user: $countEqual
    |Unknown passwords:      $countUnknown
  """.stripMargin)

val durationRDDCache = (System.nanoTime - initTime) / 1e9d

print(s"duration in seconds: $durationRDDCache")

// COMMAND ----------

// MAGIC %md 
// MAGIC # DataFrame: Mejorando el tiempo con cache
// MAGIC 
// MAGIC - si haces 3 veces el count, tiene sentido pensar que el cache te mejorará los tiempos, pruébalo

// COMMAND ----------

val initTime = System.nanoTime


val dfCached = df.cache


val countEmpty = ???
val countEqual = ???
val countUnknown = ???


val durationDFCache = (System.nanoTime - initTime) / 1e9d

print(s"duration in seconds: $durationDFCache")

// COMMAND ----------

// MAGIC %md 
// MAGIC # Mejorando el tiempo con proceso optimizado
// MAGIC 
// MAGIC - Para evitar hacer 3 veces el filter

// COMMAND ----------

val initTime = System.nanoTime


val (countEmpty, countEqual, countUnknown) = rdd
.map{case (username, password) => if (password == "") (1,0,0) 
                                   else if (username == password) (0,1,0) 
                                   else (0,0,1)}
.reduce((t1, t2) => (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3))



val durationSingleAction = (System.nanoTime - initTime) / 1e9d

print(s"duration in seconds: $durationSingleAction")

// COMMAND ----------




display(List(("rdd", durationRDD), ("df", durationDF),("durationRDDCache", durationRDDCache),  ("durationDFCache", durationDFCache), ("SA", durationSingleAction)).toDF("type", "time"))
