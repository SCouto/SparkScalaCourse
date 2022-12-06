// Databricks notebook source
// MAGIC %md
// MAGIC 
// MAGIC
// MAGIC
// MAGIC # Alturas
// MAGIC
// MAGIC El objetivo es calcular la altura media por sexo
// MAGIC
// MAGIC * Sube el fichero alturas.csv en Databricks, seguramente ya lo tengas subido de ejercicios anteriores
// MAGIC * Comprueba el fichero, verás que es de la siguiente forma:
// MAGIC
// MAGIC ```
// MAGIC H,178
// MAGIC M,179
// MAGIC H,1.6
// MAGIC ```
// MAGIC
// MAGIC * Ahora debes usar otro método para cargar el fichero
// MAGIC     * spark.read.csv
// MAGIC * Este método ya sabe como separar los registros por lo que no necesitas hacer el split. (Si el separador fuese otro que no fuera la coma habría que indicárselo)
// MAGIC * Utiliza el método WithColumn para actualizar la columna altura
// MAGIC   * Debes usar el método [when](https://sparkbyexamples.com/spark/spark-case-when-otherwise-example/) a modo de if para bifurcar los casos que vengan en metros de los que vengan en centímetros
// MAGIC   * Tendrás que convertir a Double, la sintaxis es $"columnName".cast(DoubleType)
// MAGIC * Filtra los datos erróneos (vacíos o negativos) mediante el métdo where
// MAGIC * Agrega por clave utilizando groupBy indicándole la columna de agregación
// MAGIC * Usa la función agg y avg para obtener la media

// COMMAND ----------

val inputPath = "dbfs:/FileStore/input/alturas/alturas.csv"

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


//Alturas DF

$"altura".cast(DoubleType)

val avgDF = ???

display(avgDF)

// COMMAND ----------

// MAGIC %md
// MAGIC Extraer valor concreto de un DataFrame

// COMMAND ----------

val avgMen = avgDF.filter($"sexo" === "H").first.getDouble(1)
val avgWomen = avgDF.filter($"sexo" === "M").first.getDouble(1)

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC **Ejemplo UDF**
// MAGIC
// MAGIC - 1 para renombrar H a hombre y M a Mujer
// MAGIC - Otra para clasificar a la gente en Alto/a o Bajo/a

// COMMAND ----------


val heightDF = spark.read
  .option("inferSchema", true)
  .csv(inputPath)
  .toDF(List("sexo", "altura"):_*)
  .withColumn("altura", when($"altura" < 3, $"altura" * 100).otherwise($"altura"))
  .where("altura is not null and altura > 0")

val toWord= udf((s: String) => {
  if (s == "H") "Hombre"
  else "Mujer"
})

val clusteringUsers= udf((s: String, a: Double) => {
  val group = s match {
    case "H" if a > avgMen => "Alto"
    case "H" if a < avgMen => "Bajo"
    case "M" if a > avgWomen => "Alta"
    case "M" if a < avgWomen => "Baja"
    case _ => "En la media"
  }

  println(s"sexo: $s, altura: $a, group: $group")

  group
})

display(heightDF)

val res = heightDF.withColumn("group", clusteringUsers($"sexo", $"altura"))
  .withColumn("sexo", toWord($"sexo"))


display(res.groupBy($"group").count())

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC # Alturas UDF
// MAGIC * Genera ahora una UDF que reciba una columna con un valor de altura (como String o Double) y lo estandarice
// MAGIC * Usa la UDF en lugar del engorroso when

// COMMAND ----------

//AlturasUDF


import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/*
val mtocm = udf((altura: ???) =>  {

})*/

import spark.implicits._

val avgDF2 = ???

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC **Ejemplo Spark SQL**
// MAGIC  - Registro tabla
// MAGIC  - Consulta tabla completa con spark.table
// MAGIC  - Consulta tabla con query con spark.sql

// COMMAND ----------

val df = List(("sergio", 36), ("Ester", 34)).toDF("nombre","edad")


df.createOrReplaceTempView("personas")


spark.table("personas").show

val dfResult = spark.sql("select * from personas where edad > 35")

dfResult.show

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC # Alturas SQL
// MAGIC * Registra la tabla como vista dentro de la sparkSession
// MAGIC * Copia/Usa la UDF del ejercicio anterior
// MAGIC * Registra la UDF con spark.sqlContext.udf.register(nombre, funcion)
// MAGIC * Ejecuta una query para calcular la media

// COMMAND ----------

//Alturas SQL

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._



val rawDF = spark
  .read
  .csv(inputPath)
  .toDF(List("sexo", "altura"): _*)


val resultDF = spark.sql("""
  """)

display(resultDF)