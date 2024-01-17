# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, LongType

base_path = "dbfs:/FileStore/input/movies"
output_path = "dbfs:/FileStore/output/movies"
users_path = f"{base_path}/users.dat"
movies_path = f"{base_path}/movies.dat"
ratings_path = f"{base_path}/ratings.dat"

dbutils.fs.rm(output_path, True)

#Load data
movies_df = spark.read.option("delimiter", "::").option("inferSchema", True).option("header", True).csv(movies_path)
users_df = spark.read.option("delimiter", "::").option("inferSchema", True).option("header", True).csv(users_path)
ratings_df = spark.read.option("delimiter", "::").option("inferSchema", True).option("header", True).csv(ratings_path)


# COMMAND ----------


display(ratings_df)

# COMMAND ----------

#Number of movies per year

from pyspark.sql.functions import regexp_extract
from pyspark.sql import functions as F

movies_w_year = ???
yearly_grouped = ???

display(yearly_grouped)

# COMMAND ----------

#Puntuación media por usuario
from pyspark.sql.functions import round, avg

avg_ratings_by_user = ???

display(avg_ratings_by_user)

# COMMAND ----------

#Quien puntua más alto, hombres o mujeres?

avg_ratings_by_gender = ???

display(avg_ratings_by_gender)

# COMMAND ----------

from pyspark.sql.functions import round, avg, collect_list, element_at

# Avg score by movie

movies_avg_rating = ???

display(movies_avg_rating)
