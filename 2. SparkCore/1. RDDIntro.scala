// Databricks notebook source
//Ejercicio 1
val rdd = sc.parallelize(List(("Sergio", "1985-10-05"),("Ruth", "2000-05-01")))

// COMMAND ----------

rdd.collect().foreach(println)
rdd.count

// COMMAND ----------

//Ejercicio 2 - Filter
val rddInts = sc.parallelize(1 to 50)

val rddFiltered = rddInts.filter(_ % 2== 0)

display(rddFiltered.toDF)

// COMMAND ----------

//Ejercicio 3 - Map
val rddMapped = rddFiltered.map(_ *2)

display(rddMapped.toDF)

// COMMAND ----------

//Ejercicio 3 - FlatMap
val rddSentences = sc.parallelize(List("Hola amigos", "Estoy aqui", "Con vosotros", "Muy contento"))

val rddWords = rddSentences.flatMap(_.split(" "))


display(rddWords.toDF)

// COMMAND ----------

//Ejercicio 4 - mapValues
val rddEmpleados = sc.parallelize(List(("Empleado1", 1500),("Empleado2", 2000),("Empleado3", 1700)))

//val rddEmpleadosUpdated = rddEmpleados.mapValues(_ + 500)
val rddEmpleadosUpdated = rddEmpleados.map{case (k,v) => (k, v + 500)}


display(rddEmpleadosUpdated.toDF)

// COMMAND ----------

//Ejercicio 5
val rddDepartamentos = sc.parallelize(List(("Dept1", 1500),("Dept1", 2000),("Dept2", 1700)))

val rddDepartamentosUpdated = rddDepartamentos.reduceByKey((v1, v2) => v1 + v2)

display(rddDepartamentosUpdated.toDF)