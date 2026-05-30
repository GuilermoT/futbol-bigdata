from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, trim, lower, regexp_replace, year, month, to_date, explode, lit
from pyspark.sql.types import StringType

# ================================================
# SILVER — Limpieza y transformación
# Lee desde Bronze, limpia y guarda en Silver
# ================================================

spark = SparkSession.builder \
    .appName("Futbol_Silver") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# ------------------------------------------------
# RUTAS
# ------------------------------------------------
BRONZE_PARTIDOS = "/workspaces/futbol-bigdata/ficheros/bronze/partidos"
BRONZE_MATCHES  = "/workspaces/futbol-bigdata/ficheros/bronze/matches"
BRONZE_EVENTS   = "/workspaces/futbol-bigdata/ficheros/bronze/events"
BRONZE_NOTICIAS = "/workspaces/futbol-bigdata/ficheros/bronze/noticias"

SILVER_PARTIDOS = "/workspaces/futbol-bigdata/ficheros/silver/partidos"
SILVER_MATCHES  = "/workspaces/futbol-bigdata/ficheros/silver/matches"
SILVER_EVENTS   = "/workspaces/futbol-bigdata/ficheros/silver/events"
SILVER_NOTICIAS = "/workspaces/futbol-bigdata/ficheros/silver/noticias"

# ------------------------------------------------
# 1. PARTIDOS — Limpiar y quedarse con columnas útiles
# ------------------------------------------------
print("=== Silver: Partidos ===")

df_partidos = spark.read.parquet(BRONZE_PARTIDOS)

df_partidos_silver = df_partidos.select(
    "id", "country_id", "league_id", "season",
    "stage", "date", "match_api_id",
    "home_team_api_id", "away_team_api_id",
    "home_team_goal", "away_team_goal"
).filter(
    col("home_team_goal").isNotNull() &
    col("away_team_goal").isNotNull() &
    col("date").isNotNull()
).withColumn(
    "date", to_date(col("date"))
).withColumn(
    "total_goals", col("home_team_goal") + col("away_team_goal")
).withColumn(
    "resultado",
    when(col("home_team_goal") > col("away_team_goal"), "local")
    .when(col("home_team_goal") < col("away_team_goal"), "visitante")
    .otherwise("empate")
).withColumn("year", year(col("date"))) \
 .withColumn("month", month(col("date")))

print(f"Partidos silver: {df_partidos_silver.count()}")
df_partidos_silver.show(5, truncate=False)

df_partidos_silver.write \
    .mode("overwrite") \
    .partitionBy("year", "month") \
    .parquet(SILVER_PARTIDOS)

print(f"Silver partidos guardado en: {SILVER_PARTIDOS}")

# ------------------------------------------------
# 2. MATCHES StatsBomb — Aplanar estructura JSON
# ------------------------------------------------
print("=== Silver: Matches StatsBomb ===")

df_matches = spark.read.parquet(BRONZE_MATCHES)

df_matches_silver = df_matches.select(
    col("match_id"),
    col("match_date"),
    col("kick_off"),
    col("match_week"),
    col("home_score"),
    col("away_score"),
    col("home_team.home_team_name").alias("home_team"),
    col("away_team.away_team_name").alias("away_team"),
    col("competition.competition_name").alias("competition"),
    col("season.season_name").alias("season"),
    col("stadium.name").alias("stadium")
).withColumn(
    "total_goals", col("home_score") + col("away_score")
).withColumn(
    "resultado",
    when(col("home_score") > col("away_score"), "local")
    .when(col("home_score") < col("away_score"), "visitante")
    .otherwise("empate")
)

print(f"Matches silver: {df_matches_silver.count()}")
df_matches_silver.show(5, truncate=False)

df_matches_silver.write.mode("overwrite").parquet(SILVER_MATCHES)
print(f"Silver matches guardado en: {SILVER_MATCHES}")

# ------------------------------------------------
# 3. EVENTS StatsBomb — Aplanar y filtrar columnas útiles
# ------------------------------------------------
print("=== Silver: Events StatsBomb ===")

df_events = spark.read.parquet(BRONZE_EVENTS)

df_events_silver = df_events.select(
    col("id"),
    col("index"),
    col("period"),
    col("minute"),
    col("second"),
    col("type.name").alias("event_type"),
    col("team.name").alias("team"),
    col("player.name").alias("player"),
    col("position.name").alias("position"),
    col("location"),
    col("possession"),
    col("possession_team.name").alias("possession_team"),
    col("play_pattern.name").alias("play_pattern"),
    col("duration"),
    col("under_pressure")
).filter(col("event_type").isNotNull())

print(f"Eventos silver: {df_events_silver.count()}")
df_events_silver.show(5, truncate=False)

df_events_silver.write.mode("overwrite").parquet(SILVER_EVENTS)
print(f"Silver events guardado en: {SILVER_EVENTS}")

# ------------------------------------------------
# 4. NOTICIAS — Limpiar texto y filtrar La Liga
# ------------------------------------------------
print("=== Silver: Noticias ===")

df_noticias = spark.read.parquet(BRONZE_NOTICIAS)

df_noticias_silver = df_noticias.filter(
    col("content").isNotNull() &
    col("title").isNotNull()
).withColumn(
    "content", trim(regexp_replace(col("content"), r"\s+", " "))
).withColumn(
    "title", trim(col("title"))
).withColumn(
    "content_lower", lower(col("content"))
).filter(
    col("content_lower").rlike("barcelona|real madrid|atletico|sevilla|valencia|laliga|la liga|espanyol")
).drop("content_lower")

print(f"Noticias silver (La Liga): {df_noticias_silver.count()}")
df_noticias_silver.show(5, truncate=False)

df_noticias_silver.write.mode("overwrite").parquet(SILVER_NOTICIAS)
print(f"Silver noticias guardado en: {SILVER_NOTICIAS}")

print("=== SILVER COMPLETADO ===")

spark.stop()