from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, sum as _sum, count, round as _round,
    max as _max, min as _min, desc, lower, regexp_replace,
    explode, split, trim
)

# ================================================
# GOLD — Agregaciones listas para análisis y Power BI
# ================================================

spark = SparkSession.builder \
    .appName("Futbol_Gold") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# ------------------------------------------------
# RUTAS
# ------------------------------------------------
SILVER_PARTIDOS = "/workspaces/futbol-bigdata/ficheros/silver/partidos"
SILVER_MATCHES  = "/workspaces/futbol-bigdata/ficheros/silver/matches"
SILVER_EVENTS   = "/workspaces/futbol-bigdata/ficheros/silver/events"
SILVER_NOTICIAS = "/workspaces/futbol-bigdata/ficheros/silver/noticias"

BRONZE_TEAMS     = "/workspaces/futbol-bigdata/ficheros/bronze/teams"
BRONZE_LEAGUES   = "/workspaces/futbol-bigdata/ficheros/bronze/leagues"
BRONZE_COUNTRIES = "/workspaces/futbol-bigdata/ficheros/bronze/countries"

GOLD_STATS_LIGA        = "/workspaces/futbol-bigdata/ficheros/gold/stats_liga"
GOLD_STATS_EQUIPOS     = "/workspaces/futbol-bigdata/ficheros/gold/stats_equipos"
GOLD_EVENTOS_TIPOS     = "/workspaces/futbol-bigdata/ficheros/gold/eventos_tipos"
GOLD_JUGADORES         = "/workspaces/futbol-bigdata/ficheros/gold/jugadores"
GOLD_JUGADORES_EVENTOS = "/workspaces/futbol-bigdata/ficheros/gold/jugadores_eventos"
GOLD_NOTICIAS          = "/workspaces/futbol-bigdata/ficheros/gold/noticias"
GOLD_CLUSTERS          = "/workspaces/futbol-bigdata/ficheros/gold/clusters_equipos"
GOLD_CLUSTERS_JUG      = "/workspaces/futbol-bigdata/ficheros/gold/clusters_jugadores"
GOLD_PALABRAS          = "/workspaces/futbol-bigdata/ficheros/gold/palabras_clave"
GOLD_CRUCE             = "/workspaces/futbol-bigdata/ficheros/gold/cruce_jugadores_prensa"

# ------------------------------------------------
# Cargar tablas auxiliares
# ------------------------------------------------
df_teams     = spark.read.parquet(BRONZE_TEAMS)
df_leagues   = spark.read.parquet(BRONZE_LEAGUES)
df_countries = spark.read.parquet(BRONZE_COUNTRIES)

# ------------------------------------------------
# 1. STATS POR TEMPORADA Y LIGA — con nombres
# ------------------------------------------------
print("=== Gold: Stats por temporada ===")

df_partidos = spark.read.parquet(SILVER_PARTIDOS)

df_stats_liga = df_partidos.groupBy("season", "league_id", "country_id").agg(
    count("*").alias("total_partidos"),
    _round(avg("total_goals"), 2).alias("media_goles"),
    _round(avg("home_team_goal"), 2).alias("media_goles_local"),
    _round(avg("away_team_goal"), 2).alias("media_goles_visitante"),
    _sum("total_goals").alias("total_goles")
).join(
    df_leagues.select(col("id").alias("league_id"), col("name").alias("liga")),
    on="league_id", how="left"
).join(
    df_countries.select(col("id").alias("country_id"), col("name").alias("pais")),
    on="country_id", how="left"
).orderBy("season", "liga")

print(f"Stats liga: {df_stats_liga.count()} filas")
df_stats_liga.show(5, truncate=False)
df_stats_liga.write.mode("overwrite").parquet(GOLD_STATS_LIGA)
print(f"Gold stats liga guardado en: {GOLD_STATS_LIGA}")

# ------------------------------------------------
# 2. STATS POR EQUIPO — con nombres
# ------------------------------------------------
print("=== Gold: Stats por equipo ===")

df_stats_equipos = df_partidos.groupBy("home_team_api_id").agg(
    count("*").alias("partidos_local"),
    _round(avg("home_team_goal"), 2).alias("media_goles_marcados"),
    _round(avg("away_team_goal"), 2).alias("media_goles_recibidos"),
    _sum("home_team_goal").alias("total_goles_marcados"),
    _sum("away_team_goal").alias("total_goles_recibidos")
).join(
    df_teams.select(
        col("team_api_id").alias("home_team_api_id"),
        col("team_long_name").alias("nombre_equipo")
    ),
    on="home_team_api_id", how="left"
).orderBy(desc("total_goles_marcados"))

print(f"Stats equipos: {df_stats_equipos.count()} filas")
df_stats_equipos.show(5, truncate=False)
df_stats_equipos.write.mode("overwrite").parquet(GOLD_STATS_EQUIPOS)
print(f"Gold stats equipos guardado en: {GOLD_STATS_EQUIPOS}")

# ------------------------------------------------
# 3. TIPOS DE EVENTOS StatsBomb
# ------------------------------------------------
print("=== Gold: Tipos de eventos ===")

df_events = spark.read.parquet(SILVER_EVENTS)

df_eventos_tipos = df_events.groupBy("event_type", "team").agg(
    count("*").alias("total_eventos"),
    _round(avg("duration"), 2).alias("duracion_media")
).orderBy(desc("total_eventos"))

print(f"Tipos de eventos: {df_eventos_tipos.count()} filas")
df_eventos_tipos.show(10, truncate=False)
df_eventos_tipos.write.mode("overwrite").parquet(GOLD_EVENTOS_TIPOS)
print(f"Gold eventos tipos guardado en: {GOLD_EVENTOS_TIPOS}")

# ------------------------------------------------
# 4. STATS POR JUGADOR — sin duplicados
# ------------------------------------------------
print("=== Gold: Stats por jugador ===")

df_jugadores = df_events.filter(
    col("player").isNotNull()
).groupBy("player", "team").agg(
    count("*").alias("total_acciones"),
    _round(avg("duration"), 2).alias("duracion_media_accion")
).orderBy(desc("total_acciones"))

print(f"Jugadores: {df_jugadores.count()} filas")
df_jugadores.show(10, truncate=False)
df_jugadores.write.mode("overwrite").parquet(GOLD_JUGADORES)
print(f"Gold jugadores guardado en: {GOLD_JUGADORES}")

# ------------------------------------------------
# 4b. CRUCE JUGADORES + MENCIONES EN PRENSA
# ------------------------------------------------
print("=== Gold: Cruce jugadores y prensa ===")

df_noticias_raw = spark.read.parquet(SILVER_NOTICIAS)

jugadores_barcelona = [
    "Messi", "Busquets", "Alba", "Lenglet", "Dembele",
    "Griezmann", "De Jong", "Ter Stegen", "Pique", "Pedri"
]

menciones = []
for jugador in jugadores_barcelona:
    n = df_noticias_raw.filter(
        lower(col("content")).contains(jugador.lower())
    ).count()
    menciones.append((jugador, n))

df_menciones = spark.createDataFrame(menciones, ["jugador", "menciones_prensa"])

df_cruce = df_jugadores.join(
    df_menciones,
    df_jugadores.player.contains(df_menciones.jugador),
    how="left"
).select(
    "player", "team", "total_acciones",
    "duracion_media_accion", "menciones_prensa"
).filter(col("menciones_prensa").isNotNull())

print("=== Cruce jugadores + prensa ===")
df_cruce.show(truncate=False)
df_cruce.write.mode("overwrite").parquet(GOLD_CRUCE)
print("Gold cruce jugadores prensa guardado")

# ------------------------------------------------
# 5. STATS POR JUGADOR Y TIPO DE EVENTO
# ------------------------------------------------
print("=== Gold: Stats por jugador y evento ===")

df_jugadores_eventos = df_events.filter(
    col("player").isNotNull() &
    col("event_type").isNotNull()
).groupBy("player", "team", "event_type").agg(
    count("*").alias("total_eventos"),
    _round(avg("duration"), 2).alias("duracion_media")
).orderBy(desc("total_eventos"))

print(f"Jugadores x eventos: {df_jugadores_eventos.count()} filas")
df_jugadores_eventos.show(10, truncate=False)
df_jugadores_eventos.write.mode("overwrite").parquet(GOLD_JUGADORES_EVENTOS)
print(f"Gold jugadores eventos guardado en: {GOLD_JUGADORES_EVENTOS}")

# ------------------------------------------------
# 6. NOTICIAS GOLD
# ------------------------------------------------
print("=== Gold: Noticias ===")

df_noticias = spark.read.parquet(SILVER_NOTICIAS)

df_noticias_gold = df_noticias.select(
    "title", "content", "author", "source", "publish-time"
).filter(col("content").isNotNull())

print(f"Noticias gold: {df_noticias_gold.count()} filas")
df_noticias_gold.write.mode("overwrite").parquet(GOLD_NOTICIAS)
print(f"Gold noticias guardado en: {GOLD_NOTICIAS}")

# ------------------------------------------------
# 7. PALABRAS CLAVE
# ------------------------------------------------
print("=== Gold: Palabras clave ===")

stopwords = [
    "the", "a", "an", "and", "or", "but", "in", "on", "at", "to",
    "for", "of", "with", "his", "her", "their", "that", "this",
    "was", "were", "is", "are", "be", "been", "has", "have", "had",
    "he", "she", "they", "it", "as", "by", "from", "not", "we",
    "who", "which", "will", "would", "could", "said", "also", "more"
]

df_palabras = df_noticias_gold \
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

df_palabras.show(20, truncate=False)
df_palabras.write.mode("overwrite").parquet(GOLD_PALABRAS)
print(f"Palabras clave guardado en: {GOLD_PALABRAS}")

print("=== GOLD COMPLETADO ===")

spark.stop()