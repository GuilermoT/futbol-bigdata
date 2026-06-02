from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, avg, desc, lower, regexp_replace,
    split, explode, trim
)
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator

# ================================================
# ANÁLISIS AVANZADO — ML + NLP básico
# ================================================

spark = SparkSession.builder \
    .appName("Futbol_ML") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# ------------------------------------------------
# RUTAS
# ------------------------------------------------
GOLD_STATS_EQUIPOS = "/workspaces/futbol-bigdata/ficheros/gold/stats_equipos"
GOLD_JUGADORES     = "/workspaces/futbol-bigdata/ficheros/gold/jugadores"
GOLD_NOTICIAS      = "/workspaces/futbol-bigdata/ficheros/gold/noticias"
BRONZE_TEAMS       = "/workspaces/futbol-bigdata/ficheros/bronze/teams"

GOLD_CLUSTERS      = "/workspaces/futbol-bigdata/ficheros/gold/clusters_equipos"
GOLD_CLUSTERS_JUG  = "/workspaces/futbol-bigdata/ficheros/gold/clusters_jugadores"
GOLD_PALABRAS      = "/workspaces/futbol-bigdata/ficheros/gold/palabras_clave"

# ================================================
# 1. KMEANS — Clustering de equipos por rendimiento
# ================================================
print("=== KMeans: Clustering de equipos ===")

df_equipos = spark.read.parquet(GOLD_STATS_EQUIPOS)
df_teams   = spark.read.parquet(BRONZE_TEAMS)

assembler = VectorAssembler(
    inputCols=[
        "media_goles_marcados",
        "media_goles_recibidos",
        "partidos_local"
    ],
    outputCol="features_raw"
)

df_features = assembler.transform(df_equipos)

scaler = StandardScaler(
    inputCol="features_raw",
    outputCol="features",
    withStd=True,
    withMean=True
)

scaler_model = scaler.fit(df_features)
df_scaled = scaler_model.transform(df_features)

kmeans = KMeans(
    featuresCol="features",
    predictionCol="cluster",
    k=3,
    seed=42
)

modelo = kmeans.fit(df_scaled)
df_clusters = modelo.transform(df_scaled)

print("=== Centros de los clusters ===")
for i, centro in enumerate(modelo.clusterCenters()):
    print(f"Cluster {i}: {centro}")

print("\n=== Equipos por cluster ===")
df_clusters.groupBy("cluster").agg(
    count("*").alias("num_equipos"),
    avg("media_goles_marcados").alias("media_goles_marcados"),
    avg("media_goles_recibidos").alias("media_goles_recibidos")
).orderBy("cluster").show(truncate=False)

evaluator = ClusteringEvaluator(
    featuresCol="features",
    predictionCol="cluster",
    metricName="silhouette",
    distanceMeasure="squaredEuclidean"
)

silhouette = evaluator.evaluate(df_clusters)
print(f"Silhouette Score: {silhouette:.4f}")

df_clusters.select(
    "home_team_api_id", "cluster",
    "media_goles_marcados", "media_goles_recibidos",
    "partidos_local", "nombre_equipo"
).write.mode("overwrite").parquet(GOLD_CLUSTERS)
print(f"Clusters equipos guardado en: {GOLD_CLUSTERS}")

# ================================================
# 2. KMEANS — Clustering de jugadores por actividad
# ================================================
print("\n=== KMeans: Clustering de jugadores ===")

df_jugadores = spark.read.parquet(GOLD_JUGADORES)

assembler_jug = VectorAssembler(
    inputCols=["total_acciones", "duracion_media_accion"],
    outputCol="features_raw"
)

df_jug_features = assembler_jug.transform(
    df_jugadores.filter(col("duracion_media_accion").isNotNull())
)

scaler_jug = StandardScaler(
    inputCol="features_raw",
    outputCol="features",
    withStd=True,
    withMean=True
)

df_jug_scaled = scaler_jug.fit(df_jug_features).transform(df_jug_features)

kmeans_jug = KMeans(
    featuresCol="features",
    predictionCol="cluster",
    k=3,
    seed=42
)

modelo_jug = kmeans_jug.fit(df_jug_scaled)
df_clusters_jug = modelo_jug.transform(df_jug_scaled)

print("=== Jugadores por cluster ===")
df_clusters_jug.groupBy("cluster").agg(
    count("*").alias("num_jugadores"),
    avg("total_acciones").alias("media_acciones"),
    avg("duracion_media_accion").alias("media_duracion")
).orderBy("cluster").show(truncate=False)

silhouette_jug = evaluator.evaluate(df_clusters_jug)
print(f"Silhouette Jugadores: {silhouette_jug:.4f}")

df_clusters_jug.select(
    "player", "team", "position", "cluster",
    "total_acciones", "duracion_media_accion"
).write.mode("overwrite").parquet(GOLD_CLUSTERS_JUG)
print(f"Clusters jugadores guardado en: {GOLD_CLUSTERS_JUG}")

# ================================================
# 3. NLP — Palabras clave en noticias de La Liga
# ================================================
print("\n=== NLP: Palabras clave en noticias ===")

df_noticias = spark.read.parquet(GOLD_NOTICIAS)

stopwords = [
    "the", "a", "an", "and", "or", "but", "in", "on", "at", "to",
    "for", "of", "with", "his", "her", "their", "that", "this",
    "was", "were", "is", "are", "be", "been", "has", "have", "had",
    "he", "she", "they", "it", "as", "by", "from", "not", "we",
    "who", "which", "will", "would", "could", "said", "also", "more"
]

df_palabras = df_noticias \
    .withColumn("content_clean",
        regexp_replace(lower(col("content")), r"[^a-zA-Z\s]", "")
    ) \
    .withColumn("palabra", explode(split(col("content_clean"), r"\s+"))) \
    .withColumn("palabra", trim(col("palabra"))) \
    .filter(col("palabra") != "") \
    .filter(~col("palabra").isin(stopwords)) \
    .filter(col("palabra").rlike("^[a-z]{4,}$")) \
    .groupBy("palabra") \
    .agg(count("*").alias("frecuencia")) \
    .orderBy(desc("frecuencia"))

print("=== Top 20 palabras más frecuentes en noticias de La Liga ===")
df_palabras.show(20, truncate=False)

df_palabras.write.mode("overwrite").parquet(GOLD_PALABRAS)
print(f"Palabras clave guardado en: {GOLD_PALABRAS}")

print("\n=== ANÁLISIS ML COMPLETADO ===")

spark.stop()