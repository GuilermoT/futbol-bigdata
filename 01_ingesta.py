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
RAW_SQLITE   = "/workspaces/futbol-bigdata/ficheros/raw/sqlite/database.sqlite"
RAW_JSON_MATCHES   = "/workspaces/futbol-bigdata/ficheros/raw/json/matches_laliga.json"
RAW_JSON_EVENTS    = "/workspaces/futbol-bigdata/ficheros/raw/json/events"
RAW_JSON_COMPETITIONS = "/workspaces/futbol-bigdata/ficheros/raw/json/competitions.json"
RAW_CSV      = "/workspaces/futbol-bigdata/ficheros/raw/csv/final-articles.csv"

BRONZE_PARTIDOS  = "/workspaces/futbol-bigdata/ficheros/bronze/partidos"
BRONZE_MATCHES   = "/workspaces/futbol-bigdata/ficheros/bronze/matches"
BRONZE_EVENTS    = "/workspaces/futbol-bigdata/ficheros/bronze/events"
BRONZE_NOTICIAS  = "/workspaces/futbol-bigdata/ficheros/bronze/noticias"

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
df_partidos.write.mode("overwrite").parquet(BRONZE_PARTIDOS)
print(f"Bronze partidos guardado en: {BRONZE_PARTIDOS}")

# Tablas auxiliares SQLite
print("=== Leyendo tablas auxiliares SQLite ===")

df_teams = spark.read \
    .format("jdbc") \
    .option("url", f"jdbc:sqlite:{RAW_SQLITE}") \
    .option("dbtable", "Team") \
    .option("driver", "org.sqlite.JDBC") \
    .load()

print(f"Equipos cargados: {df_teams.count()}")
df_teams.write.mode("overwrite").parquet("/workspaces/futbol-bigdata/ficheros/bronze/teams")

df_leagues = spark.read \
    .format("jdbc") \
    .option("url", f"jdbc:sqlite:{RAW_SQLITE}") \
    .option("dbtable", "League") \
    .option("driver", "org.sqlite.JDBC") \
    .load()

print(f"Ligas cargadas: {df_leagues.count()}")
df_leagues.write.mode("overwrite").parquet("/workspaces/futbol-bigdata/ficheros/bronze/leagues")

df_countries = spark.read \
    .format("jdbc") \
    .option("url", f"jdbc:sqlite:{RAW_SQLITE}") \
    .option("dbtable", "Country") \
    .option("driver", "org.sqlite.JDBC") \
    .load()

print(f"Países cargados: {df_countries.count()}")
df_countries.write.mode("overwrite").parquet("/workspaces/futbol-bigdata/ficheros/bronze/countries")

# ------------------------------------------------
# 2. JSON — StatsBomb matches + events
# ------------------------------------------------
print("=== Leyendo JSON Matches ===")

df_matches = spark.read \
    .option("multiline", "true") \
    .json(RAW_JSON_MATCHES)

print(f"Matches cargados: {df_matches.count()}")
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
df_noticias.write.mode("overwrite").parquet(BRONZE_NOTICIAS)
print(f"Bronze noticias guardado en: {BRONZE_NOTICIAS}")

print("=== INGESTA BRONZE COMPLETADA ===")

spark.stop()