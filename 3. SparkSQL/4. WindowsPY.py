# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, FloatType, StringType, IntegerType

data_schema = ["generation", "product", "category", "revenue"]


raw_data = [(1.0, "Thin", "cell phone", 6000),
        (1.0, "Normal", "tablet", 1500),
        (2.0, "Mini", "tablet", 5500),
        (3.0, "Ultra thin", "cell phone", 5000),
        (2.0, "Very thin", "cell phone", 6000),
        (2.0, "Big", "tablet", 2500),
        (2.0, "Bendable", "cell phone", 3000),
        (2.0, "Foldable", "cell phone", 3000),
        (3.0, "Pro", "tablet", 4500),
        (4.0, "Pro2", "tablet", 6500)]

data = spark.createDataFrame(raw_data, schema=data_schema)

display(data)

# COMMAND ----------

#Ejercicio 1 - Máximo por categoría
from pyspark.sql import Window
from pyspark.sql.functions import col, max

over_category = ???

# Calculate maximum revenue per category and filter the top-selling products
top_sell = ???

display(top_sell)

# COMMAND ----------

#Ejercicio 2 - top 2 ventas por categoría

from pyspark.sql import Window
from pyspark.sql.functions import col, row_number

ranked = ???

display(ranked)

# COMMAND ----------

#Ejercicio 3 - Diferencia entre producto y top venta

ranked = ???

display(ranked)
