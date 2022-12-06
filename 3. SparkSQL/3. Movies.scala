// Databricks notebook source
val basePath =  "dbfs:/FileStore/input/movies"
val outputPath =  "dbfs:/FileStore/output/movies"
val usersPath = s"$basePath/users.dat"
val moviesPath = s"$basePath/movies.dat"
val ratingsPath = s"$basePath/ratings.dat"

//Aux
//case class UserRating (userId: Int, gender: String, age: Int, occupation: String, zipcode: String, movie: Int, score: Int, tm: Long)


case class Movie(id: Int, name: String, genre: String)
case class User(userId: Int, gender: String, age: Int, occupation: String, zipcode: String)
case class Rating(userId: Int, movie: Int, score: Int, tm: Long)


import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// COMMAND ----------

//Load data

val moviesDS = spark.read.option("delimiter", "::").option("inferSchema", "true").option("header", "true").csv(moviesPath).as[Movie]
val usersDS = spark.read.option("delimiter", "::").option("inferSchema", "true").option("header", "true").csv(usersPath).as[User]
val ratingsDS = spark.read.option("delimiter", "::").option("inferSchema", "true").option("header", "true").csv(ratingsPath).as[Rating]


display(moviesDS)

// COMMAND ----------

//Películas por año
val moviesWYear = ???

val yearlyGrouped = ???

display(yearlyGrouped)


// COMMAND ----------
//Writing output


dbutils.fs.rm(outputPath, true)
spark.sql("drop table if exists moviesByYear")

moviesWYear.write.mode(SaveMode.Overwrite).partitionBy("year").csv(outputPath)

moviesWYear.write.format("delta").partitionBy("year").saveAsTable("moviesByYear")



// COMMAND ----------


// MAGIC %sql
// MAGIC --Seleccionando todo
// MAGIC select * from default.moviesByYear

// COMMAND ----------

// MAGIC %sql
// MAGIC --Seleccionando filtro por año
// MAGIC select * from default.moviesByYear
// MAGIC where year = 1985

// COMMAND ----------
//Eliminando salida
dbutils.fs.rm(outputPath, true)
spark.sql("drop table if exists default.moviesByYear")

// COMMAND ----------

//Average rating by user
val avgRatingsByUser = ???



display(avgRatingsByUser)

// COMMAND ----------

//Quien puntua más alto, hombres o mujeres?
val avgRatingsByGender = ???


display(avgRatingsByGender)

// COMMAND ----------

//average score by movie
val moviesAvgRating = ???

display(moviesAvgRating)
