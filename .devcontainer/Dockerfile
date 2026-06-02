FROM python:3.11-slim-bookworm

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      ca-certificates \
      curl \
      gnupg \
      openjdk-17-jre-headless \
 && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir pyspark numpy

# Driver SQLite para Spark
RUN curl -L https://github.com/xerial/sqlite-jdbc/releases/download/3.45.1.0/sqlite-jdbc-3.45.1.0.jar \
    -o /tmp/sqlite-jdbc.jar && \
    find / -name "jars" -path "*/pyspark/*" 2>/dev/null | head -1 | xargs -I{} cp /tmp/sqlite-jdbc.jar {}/sqlite-jdbc.jar

WORKDIR /work