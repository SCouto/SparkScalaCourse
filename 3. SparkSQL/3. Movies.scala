// Databricks notebook source
val basePath =  "dbfs:/FileStore/input/movies"
val outputPath =  "dbfs:/FileStore/output/movies"
val usersPath = s"$basePath/users.dat"
val moviesPath = s"$basePath/movies.dat"
val ratingsPath = s"$basePath/ratings.dat"

dbutils.fs.rm(outputPath, true)


//Aux
case class UserRating (userId: Int, gender: String, age: Int, occupation: String, zipcode: String, movie: Int, score: Int, tm: Long)


import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// COMMAND ----------

//Load data



// COMMAND ----------

//Películas por año
val moviesWYear = ???
val yearlyGrouped = ???

display(yearlyGrouped)


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