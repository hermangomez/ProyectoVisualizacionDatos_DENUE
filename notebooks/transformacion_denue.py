# Databricks notebook source

from pyspark.sql import functions as F

ruta="/Volumes/dataengineering/visualizacion/denue"

df=spark.read.csv(ruta,
                  header=True,
                  inferSchema=True)
display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------



# COMMAND ----------

from pyspark.sql import functions as F


df_numerico = df.withColumn(
    "personal_ocupado",
    F.when(F.col("per_ocu")=="0 a 5 personas",2.5)
    .when(F.col("per_ocu")=="6 a 10 personas", 8)
    .when(F.col("per_ocu")=="11 a 30 personas", 20.5)
    .when(F.col("per_ocu")=="31 a 50 personas", 40.5)
    .when(F.col("per_ocu")=="51 a 100 personas", 75)
    .when(F.col("per_ocu")=="101 a 250 personas", 150)
    .when(F.col("per_ocu")=="251 y más personas", 175)
    .otherwise(None)
)


resumen = (
    df_numerico.groupBy("municipio")
    .agg(
    F.count("*").alias("Total Unidades"),
    F.sum("personal_ocupado").alias("Personal_estimado_total"),
    F.countDistinct("nombre_act ").alias("diversidad_actividades")
         )
)
display(resumen)


# COMMAND ----------

# DBTITLE 1,Install plotly
# MAGIC %uv pip install plotly

# COMMAND ----------

import plotly.express as px

pdf = resumen.toPandas()

fig = px.scatter(
    pdf,
    x="Total Unidades",
    y="Personal_estimado_total",
    size="diversidad_actividades",
    color="diversidad_actividades",
    hover_name="municipio",
    size_max=50,
    title="Análisis multivariable de unidades económicas",
    labels={
        "Total Unidades": "Total de unidades económicas",
        "Personal_estimado_total": "Personal ocupado estimado",
        "diversidad_actividades": "Diversidad de actividades"
    }
)
fig.update_xaxes(type="log")
fig.update_yaxes(type="log")
fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## VISUALIZACION GEOESPACIAL

# COMMAND ----------

df_mapa = (
    df.withColumn(
        "latitud_num",
        F.col("Latitud").cast("double")
    )
    .withColumn(
        "longitud_num",
        F.col("Longitud").cast("double")
    )
)

display(df_mapa.select(
    "cve_mun", "latitud_num", "longitud_num"
).limit(10))

# COMMAND ----------

pip install px

# COMMAND ----------

import plotly.express as px

pdf = (
    df_mapa.select(
        "cve_mun",
        "latitud_num",
        "longitud_num"
    )
    .limit(1000)
    .toPandas()
)

fig = px.scatter_map(
    pdf,
    lat="latitud_num",
    lon="longitud_num",
    color="cve_mun",
    hover_name="cve_mun",
    zoom=8,
    title="Distribución geográfica de unidades económicas"
)

fig.show()

# COMMAND ----------

from pyspark.sql import functions as F

serie_tiempo = (
    df.withColumn(
        "fecha",
        F.to_date(
            F.concat(F.col("fecha_alta"), F.lit("-01"))
        )
    )
    .filter(F.col("fecha").isNotNull())
    .groupBy("fecha")
    .agg(
        F.count("*").alias("unidades_incorporadas")
    )
    .orderBy("fecha")
)

display(serie_tiempo)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Distribucion de grupos

# COMMAND ----------

from pyspark.sql import functions as F

df = df.withColumn(
    "municipio",
    F.when(F.col("municipio").contains("Cos"), "Cosío")
     .when(F.col("municipio").contains("Jes"), "Jesús María")
     .when(F.col("municipio").contains("Pabell"), "Pabellón de Arteaga")
     .when(F.col("municipio").contains("Rinc"), "Rincón de Romos")
     .when(F.col("municipio").contains("San Jos"), "San José de Gracia")
     .when(F.col("municipio").contains("Tepez"), "Tepezalá")
     .otherwise(F.col("municipio"))
)

#display(df.select("municipio").distinct())

por_actividad=(
    df.groupBy("municipio","codigo_act")
    .agg(F.count("*").alias("total_unidades"))
)


pdf = por_actividad.toPandas()

# COMMAND ----------

import plotly.express as px

fig = px.box(
    pdf,
    x="municipio",
    y="total_unidades",
    points="outliers",
    title="Distribución de unidades económicas por municipio",
    labels={
        "municipio": "Municipio",
        "total_unidades": "Unidades por actividad económica"
    }
)

fig.update_layout(
    xaxis_tickangle=-45
)

fig.show()

# COMMAND ----------

resumen=(df.groupBy("municipio")
         .agg(
        F.count("*").alias("Total Unidades")
        ))

display(resumen)

# COMMAND ----------

from pyspark.sql import functions as F

df = df.withColumn(
    "municipio",
    F.when(F.col("municipio").contains("Cos"), "Cosío")
     .when(F.col("municipio").contains("Jes"), "Jesús María")
     .when(F.col("municipio").contains("Pabell"), "Pabellón de Arteaga")
     .when(F.col("municipio").contains("Rinc"), "Rincón de Romos")
     .when(F.col("municipio").contains("San Jos"), "San José de Gracia")
     .when(F.col("municipio").contains("Tepez"), "Tepezalá")
     .otherwise(F.col("municipio"))
)

display(df.select("municipio").distinct())

per_ocupado=(
    df.groupBy("municipio","per_ocu")
    .agg(F.count("*").alias("total_unidades"))
)

display(per_ocupado)