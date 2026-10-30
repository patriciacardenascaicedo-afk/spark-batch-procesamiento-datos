El procesamiento batch se realizó con el siguiente script en

PySpark:

from pyspark.sql import SparkSession

from pyspark.sql.functions import col, avg, when, count

 

# 1.

Inicializar la sesión de Spark

spark =

SparkSession.builder \

   .appName("Analisis_Calidad_Aire_PatriciaCardenas") \

    .getOrCreate()

 

# 2. Cargar el

conjunto de datos desde la fuente (CSV)

path_data = "calidad_aire_colombia.csv"

df_raw = spark.read.option("header", "true") \

   .option("inferSchema", "true") \

    .csv(path_data)

 

print("---

Esquema del Conjunto de Datos ---")

df_raw.printSchema()

 

# 3. Limpieza y

Transformación de Datos (EDA)

# Se eliminan

registros nulos en lecturas de contaminantes y se filtran valores erróneos

(< 0)

df_cleaned = df_raw.dropna(subset=["id_estacion",

"pm25"]) \

    .filter(col("pm25") >= 0)

 

#

Categorización del nivel de riesgo según la concentración de PM2.5

df_transformed = df_cleaned.withColumn(

    "nivel_riesgo",

    when(col("pm25") <= 12,

"Bueno")

    .when((col("pm25")

> 12) & (col("pm25") <= 37), "Moderado")

    .otherwise("Dañino para la

Salud")

)

 

# 4. Análisis

Exploratorio y Agregaciones

df_resumen =

df_transformed.groupBy("municipio") \

    .agg(

        avg("pm25").alias("promedio_pm25"),

        count("id_estacion").alias("total_lecturas")

    ) \

   .orderBy(col("promedio_pm25").desc())

 

# 5. Muestreo

de Resultados del Procesamiento Batch

print("---

Registros Procesados con Nivel de Riesgo ---")

df_transformed.select("municipio",

"pm25", "nivel_riesgo").show(10, truncate=False)

print("---

Promedio de PM2.5 por Municipio ---")

df_resumen.show(truncate=False)

 

# Finalizar la

sesión de Spark

spark.stop()