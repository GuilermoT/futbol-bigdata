from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, sum as _sum, count, round as _round,
    max as _max, min as _min, desc
)

# ================================================
# GOLD — Agregaciones listas para análisis y Power BI
# Lee desde Silver y genera tablas de negocio
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

GOLD_STATS_LIGA     = "/workspaces/futbol-bigdata/ficheros/gold/stats_liga"
GOLD_STATS_EQUIPOS  = "/workspaces/futbol-bigdata/ficheros/gold/stats_equipos"
GOLD_EVENTOS_TIPOS  = "/workspaces/futbol-bigdata/ficheros/gold/eventos_tipos"
GOLD_JUGADORES      = "/workspaces/futbol-bigdata/ficheros/gold/jugadores"
GOLD_NOTICIAS       = "/workspaces/futbol-bigdata/ficheros/gold/noticias"

# ------------------------------------------------
# 1. STATS POR TEMPORADA Y LIGA
# ------------------------------------------------
print("=== Gold: Stats por temporada ===")

df_partidos = spark.read.parquet(SILVER_PARTIDOS)

df_stats_liga = df_partidos.groupBy("season", "league_id").agg(
    count("*").alias("total_partidos"),
    _round(avg("total_goals"), 2).alias("media_goles"),
    _round(avg("home_team_goal"), 2).alias("media_goles_local"),
    _round(avg("away_team_goal"), 2).alias("media_goles_visitante"),
    _sum("total_goals").alias("total_goles"),
    count(col("resultado") == "local").alias("victorias_local"),
    count(col("resultado") == "empate").alias("empates"),
    count(col("resultado") == "visitante").alias("victorias_visitante")
).orderBy("season", "league_id")

print(f"Stats liga: {df_stats_liga.count()} filas")
df_stats_liga.show(5, truncate=False)

df_stats_liga.write.mode("overwrite").parquet(GOLD_STATS_LIGA)
print(f"Gold stats liga guardado en: {GOLD_STATS_LIGA}")

# ------------------------------------------------
# 2. STATS POR EQUIPO LOCAL
# ------------------------------------------------
print("=== Gold: Stats por equipo ===")

df_stats_equipos = df_partidos.groupBy("home_team_api_id").agg(
    count("*").alias("partidos_local"),
    _round(avg("home_team_goal"), 2).alias("media_goles_marcados"),
    _round(avg("away_team_goal"), 2).alias("media_goles_recibidos"),
    _sum("home_team_goal").alias("total_goles_marcados"),
    _sum("away_team_goal").alias("total_goles_recibidos")
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
# 4. STATS POR JUGADOR
# ------------------------------------------------
print("=== Gold: Stats por jugador ===")

df_jugadores = df_events.filter(
    col("player").isNotNull()
).groupBy("player", "team", "position").agg(
    count("*").alias("total_acciones"),
    _round(avg("duration"), 2).alias("duracion_media_accion")
).orderBy(desc("total_acciones"))

print(f"Jugadores: {df_jugadores.count()} filas")
df_jugadores.show(10, truncate=False)

df_jugadores.write.mode("overwrite").parquet(GOLD_JUGADORES)
print(f"Gold jugadores guardado en: {GOLD_JUGADORES}")

# ------------------------------------------------
# 5. NOTICIAS GOLD — listas para NLP
# ------------------------------------------------
print("=== Gold: Noticias ===")

df_noticias = spark.read.parquet(SILVER_NOTICIAS)

df_noticias_gold = df_noticias.select(
    "title", "content", "author", "source", "publish-time"
).filter(
    col("content").isNotNull()
)

print(f"Noticias gold: {df_noticias_gold.count()} filas")

df_noticias_gold.write.mode("overwrite").parquet(GOLD_NOTICIAS)
print(f"Gold noticias guardado en: {GOLD_NOTICIAS}")

print("=== GOLD COMPLETADO ===")

spark.stop()