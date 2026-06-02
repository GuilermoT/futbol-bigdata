# Análisis de Datos del Fútbol Profesional Europeo

Análisis de datos del fútbol profesional europeo mediante un Data Lake con arquitectura Bronze/Silver/Gold. Ingesta de datos estructurados, semiestructurados y no estructurados procesados con PySpark y visualizados en Power BI.

## Metodología: CRISP-DM

### 1. Business Understanding
Análisis del fútbol profesional europeo como industria impulsada por datos. El objetivo es cruzar tres tipos de fuentes para entender no solo qué pasó en los partidos, sino cómo se jugó y qué generó en prensa.

### 2. Data Understanding
Se identificaron tres fuentes de datos reales y accesibles:
- **European Soccer Database (Kaggle)** — 25.979 partidos de ligas europeas (2008-2016). Datos estructurados en SQLite.
- **StatsBomb Open Data (GitHub)** — Eventos tácticos de 5 partidos del Barcelona 2020/21. Datos semiestructurados en JSON.
- **Football News Articles (Kaggle)** — 11.963 artículos de prensa de Goal.com y SkySports. Datos no estructurados en CSV.

### 3. Data Preparation
Arquitectura por capas implementada con PySpark:
- **Bronze** — Ingesta en crudo de las 3 fuentes en formato Parquet sin transformaciones.
- **Silver** — Limpieza, normalización de tipos, filtrado de nulos y aplanado de estructuras JSON.
- **Gold** — Agregaciones y joins con tablas auxiliares (equipos, ligas, países) listas para análisis y visualización.

### 4. Modeling
- **KMeans (k=3)** sobre 299 equipos europeos agrupados por rendimiento goleador. Silhouette Score: 0.51.
- **KMeans (k=3)** sobre 139 jugadores del Barcelona agrupados por actividad. Silhouette Score: 0.59.
- **NLP** — Análisis de frecuencia de palabras sobre 4.068 noticias filtradas de La Liga.

### 5. Evaluation
Los clusters de equipos identificaron claramente 3 grupos: equipos top (20%), medios (43%) y débiles (37%). Los scores Silhouette superiores a 0.5 indican una separación aceptable entre clusters.

### 6. Deployment
Dashboard interactivo en Power BI con 3 páginas:
- **Ligas Europeas** — Evolución de goles, ranking de equipos, clustering y distribución por país.
- **Barcelona (5 partidos 2020/21)** — Análisis táctico, ranking de jugadores y clustering de actividad.
- **Prensa** — Top palabras y nube de palabras en noticias de La Liga.

## Tecnologías
- PySpark 4.1
- Python 3.11
- Docker + DevContainer
- Power BI Desktop

## Fuentes de datos
Los datasets no están incluidos por su tamaño. Descárgalos y colócalos en las rutas indicadas:

- **European Soccer Database** → https://www.kaggle.com/datasets/hugomathien/soccer → `ficheros/raw/sqlite/database.sqlite`
- **StatsBomb Open Data** → https://github.com/statsbomb/open-data → `ficheros/raw/json/`
- **Football News Articles** → https://www.kaggle.com/datasets/hammadjavaid/football-news-articles-dataset → `ficheros/raw/csv/final-articles.csv`