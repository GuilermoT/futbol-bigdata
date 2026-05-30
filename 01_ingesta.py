from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import os

# ================================================
# INGESTA - CAPA BRONZE
# Lee los 3 datasets en crudo y los guarda en
# formato Parquet en la capa Bronze
# ================================================

spark = SparkSession.builder \
    .appName("Futbol_Ingesta_Bronze") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# ------------------------------------------------
# RUTAS
# ------------------------------------------------
RAW_SQLITE = "/workspaces/futbol-bigdata/ficheros/raw/sqlite/database.sqlite"
RAW_JSON_MATCHES   = "ficheros/raw/json/matches_laliga.json"
RAW_JSON_EVENTS    = "ficheros/raw/json/events"
RAW_JSON_COMPETITIONS = "ficheros/raw/json/competitions.json"
RAW_CSV      = "ficheros/raw/csv/final-articles.csv"

BRONZE_PARTIDOS  = "ficheros/bronze/partidos"
BRONZE_MATCHES   = "ficheros/bronze/matches"
BRONZE_EVENTS    = "ficheros/bronze/events"
BRONZE_NOTICIAS  = "ficheros/bronze/noticias"

# ------------------------------------------------
# 1. SQLITE — European Soccer Database
# ------------------------------------------------
print("=== Leyendo SQLite ===")

df_partidos = spark.read \
    .format("jdbc") \
    .option("url", f"jdbc:sqlite:{RAW_SQLITE}") \
    .option("dbtable", "Match") \
    .option("driver", "org.sqlite.JDBC") \
    .load()

print(f"Partidos cargados: {df_partidos.count()}")
df_partidos.printSchema()

df_partidos.write.mode("overwrite").parquet(BRONZE_PARTIDOS)
print(f"Bronze partidos guardado en: {BRONZE_PARTIDOS}")

# ------------------------------------------------
# 2. JSON — StatsBomb matches + events
# ------------------------------------------------
print("=== Leyendo JSON Matches ===")

df_matches = spark.read \
    .option("multiline", "true") \
    .json(RAW_JSON_MATCHES)

print(f"Matches cargados: {df_matches.count()}")
df_matches.printSchema()

df_matches.write.mode("overwrite").parquet(BRONZE_MATCHES)
print(f"Bronze matches guardado en: {BRONZE_MATCHES}")

print("=== Leyendo JSON Events ===")

df_events = spark.read \
    .option("multiline", "true") \
    .json(RAW_JSON_EVENTS)

print(f"Eventos cargados: {df_events.count()}")

df_events.write.mode("overwrite").parquet(BRONZE_EVENTS)
print(f"Bronze events guardado en: {BRONZE_EVENTS}")

# ------------------------------------------------
# 3. CSV — Football News Articles
# ------------------------------------------------
print("=== Leyendo CSV Noticias ===")

df_noticias = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .option("multiline", True) \
    .option("escape", '"') \
    .csv(RAW_CSV)

print(f"Noticias cargadas: {df_noticias.count()}")
df_noticias.printSchema()

df_noticias.write.mode("overwrite").parquet(BRONZE_NOTICIAS)
print(f"Bronze noticias guardado en: {BRONZE_NOTICIAS}")

print("=== INGESTA BRONZE COMPLETADA ===")

spark.stop()