# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC # Alturas
# MAGIC
# MAGIC El objetivo es calcular la altura media por sexo
# MAGIC
# MAGIC * Sube el fichero alturas.csv en Databricks, seguramente ya lo tengas subido de ejercicios anteriores
# MAGIC * Comprueba el fichero, verás que es de la siguiente forma:
# MAGIC
# MAGIC ```
# MAGIC H,178
# MAGIC M,179
# MAGIC H,1.6
# MAGIC ```
# MAGIC
# MAGIC * Ahora debes usar otro método para cargar el fichero
# MAGIC     * spark.read.csv
# MAGIC * Este método ya sabe como separar los registros por lo que no necesitas hacer el split. (Si el separador fuese otro que no fuera la coma habría que indicárselo)
# MAGIC * Utiliza el método WithColumn para actualizar la columna altura 
# MAGIC   * Debes usar el método [when](https://sparkbyexamples.com/spark/spark-case-when-otherwise-example/) a modo de if para bifurcar los casos que vengan en metros de los que vengan en centímetros
# MAGIC   * Tendrás que convertir a Double, la sintaxis es col("altura").cast(DoubleType())
# MAGIC * Filtra los datos erróneos (vacíos o negativos) mediante el métdo where
# MAGIC * Agrega por clave utilizando groupBy indicándole la columna de agregación
# MAGIC * Usa la función agg y avg para obtener la media

# COMMAND ----------

inputPath = "dbfs:/FileStore/input/alturas/alturas.csv"

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, round, avg
from pyspark.sql.types import DoubleType


#Alturas DF


# COMMAND ----------

# MAGIC %md
# MAGIC Extraer valor concreto de un DataFrame

# COMMAND ----------


avg_men = avg_df.filter(col("sexo") == "H").select("altura").first()["altura"]
avg_women = avg_df.filter(col("sexo") == "M").select("altura").first()["altura"]

print(f"avg men {avg_men}")
print(f"avg women {avg_women}")


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **Ejemplo UDF**
# MAGIC
# MAGIC - 1 para renombrar H a hombre y M a Mujer
# MAGIC - Otra para clasificar a la gente en Alto/a o Bajo/a

# COMMAND ----------


from pyspark.sql.functions import udf
from pyspark.sql.types import StringType


height_df = spark.read.csv(inputPath, header=True, inferSchema=True) \
    .toDF("sexo", "altura") \
  .withColumn("altura", when(col("altura") < 3, col("altura").cast(DoubleType()) * 100).otherwise(col("altura").cast(DoubleType()))) \
  .where("altura is not null and altura > 0") \


@udf(StringType())
def to_word(s):
    if s=="H":
        "Hombre"
    else:
        "Mujer"



@udf(StringType())
def clustering_users(s, a):
    group = ""
    if s == "H":
        group = "Alto" if a > avg_men else "Bajo"
    elif s == "M":
        group = "Alta" if a > avg_women else "Baja"
    else:
        group = "En la media"
    
    print(f"sexo: {s}, altura: {a}, group: {group}")
    
    return group
  
res = height_df.withColumn("group", clustering_users(col("sexo"), col("altura"))) \
        .withColumn("sexo", to_word("sexo"))


display(res.groupBy("group").count())

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Alturas UDF
# MAGIC * Genera ahora una UDF que reciba una columna con un valor de altura (como String o Double) y lo estandarice
# MAGIC * Usa la UDF en lugar del engorroso when
# MAGIC

# COMMAND ----------

//AlturasUDF 

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **Ejemplo Spark SQL**
# MAGIC  - Registro tabla
# MAGIC  - Consulta tabla completa con spark.table
# MAGIC  - Consulta tabla con query con spark.sql

# COMMAND ----------

from pyspark.sql import SparkSession


data = [("sergio", 36), ("Ester", 34)]

df = spark.createDataFrame(data, ["nombre", "edad"])

df.createOrReplaceTempView("personas")

spark.table("personas").show()

df_result = spark.sql("SELECT * FROM personas WHERE edad > 35")

display(df_result)


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Alturas SQL
# MAGIC * Registra la tabla como vista dentro de la sparkSession
# MAGIC * Copia/Usa la UDF del ejercicio anterior
# MAGIC * Registra la UDF con spark.sqlContext.udf.register
# MAGIC * Ejecuta una query para calcular la media
# MAGIC

# COMMAND ----------

#Alturas SQL



