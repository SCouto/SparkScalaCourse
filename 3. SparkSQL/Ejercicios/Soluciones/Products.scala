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

//Cliente que gastase más dinero
val biggestClient = transactions
      .groupBy("clientId")
      .agg(round(sum("totalPrice"), 2).as("totalPrice"))
      .sort($"totalPrice".desc)
      .head(1).head

println("Ejercicio 1")
println(s"client with id: ${biggestClient.get(0)} has expenses with amount:  ${biggestClient.get(1)}")


// COMMAND ----------

//Informe de productos vendidos
val productsSold = transactions
      .groupBy("productId")
      .agg(round(sum("totalPrice"), 2).as("totalPrice"), sum("itemAmount").as("itemAmount"))
      .join(products.withColumnRenamed("id", "productId"), "productId")
      .drop("price", "stock")

    productsSold.show(10)

// COMMAND ----------

//Productos no vendidos
val unSoldproducts = products.withColumnRenamed("id", "productId").as("product")
     //      .join(transactions.as("trx"), Seq("productId"), "left")
     //      .where ("trx.productId is null")
     .join(transactions.as("trx"), Seq("productId"), "leftanti")
     .select($"product.productId", $"product.description", $"product.price")

unSoldproducts.show(10)

// COMMAND ----------

//Transacciones de los 10 productos con mayor stock
val topStockedProducts = products.sort($"stock".desc).limit(10)

val topStockedTransactions = transactions
      .join(topStockedProducts.withColumnRenamed("id", "productId"), "productId")

topStockedTransactions.show()