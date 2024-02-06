// Databricks notebook source
val basePath =  "dbfs:/FileStore/input/products"
val baseOutputPath =  "dbfs:/FileStore/output/products"
val productsPath = s"$basePath/products.txt"
val trxPath = s"$basePath/transactions.txt"

case class Product (id: Int, description: String, price: Double, stock: Int)
case class Transaction (date: String, time: String, clientId: Int, productId: Int, itemAmount: Int, totalPrice: Double)

// COMMAND ----------

//Load product
val productSchemaNames = List("id", "description", "price", "stock")
val productsRaw = spark.read.option("delimiter", "#").option("inferSchema", true).csv(productsPath)
val products = productSchemaNames.zip(productsRaw.schema.map(_.name)).foldLeft(productsRaw)((accDF, elem) => accDF.withColumnRenamed(elem._2, elem._1)).as[Product]


//Load transactions
val trxSchemaNames = List("date", "time", "clientId", "productId", "itemAmount", "totalPrice")
val transactionsRaw = spark.read.option("delimiter", "#").option("inferSchema", true).csv(trxPath)
val transactions = trxSchemaNames.zip(transactionsRaw.schema.map(_.name)).foldLeft(transactionsRaw)((accDF, elem) => accDF.withColumnRenamed(elem._2, elem._1)).as[Transaction]


// COMMAND ----------

import org.apache.spark.sql.functions._

//Cliente que gastase más dinero -- Output should be client 76 with 100049.0 amount
val biggestClient = ???

println(s"client with id: ${biggestClient.get(0)} has expenses with amount:  ${biggestClient.get(1)}")



// COMMAND ----------

//Informe de productos vendidos - Output should be 96 rows, product 31 to check your result:
//id:  31
//description: Product 31
//totalPrice: 75445.77
//itemAmount: 55
val productsSold = ???



// COMMAND ----------

//Productos no vendidos

//Expected Output
/*
+---------+-----------+-------+
|productId|description|  price|
+---------+-----------+-------+
|        3|  Product 3|1808.79|
|       20| Product 20|4589.79|
|       43| Product 43|2718.14|
|       63| Product 63|8131.85|
+---------+-----------+-------+
*/

val unSoldproducts = ???

display(unSoldproducts)


// COMMAND ----------

//Transacciones de los 8 productos con mayor stock - Output should be 83 rows
val topStockedProducts = ???

val topStockedTransactions = ???

topStockedTransactions.show(


