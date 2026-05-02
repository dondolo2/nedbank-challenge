FROM nedbank-de-challenge/base:1.0

WORKDIR /app
ENV PYTHONPATH=/app
# Base image may set SPARK_HOME to dist-packages; pip installs PySpark under site-packages.
ENV SPARK_HOME=/usr/local/lib/python3.11/site-packages/pyspark

# delta-spark on PyPI is Python-only; runtime needs this JAR on the classpath. Bundle at build time
# so Spark works with --network=none (no Ivy/Maven download).
ARG DELTA_SPARK_VERSION=3.1.0
RUN mkdir -p /app/spark-jars && \
    curl -fsSL -o /app/spark-jars/delta-spark_2.12-${DELTA_SPARK_VERSION}.jar \
    "https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/${DELTA_SPARK_VERSION}/delta-spark_2.12-${DELTA_SPARK_VERSION}.jar" && \
    curl -fsSL -o /app/spark-jars/delta-storage-${DELTA_SPARK_VERSION}.jar \
    "https://repo1.maven.org/maven2/io/delta/delta-storage/${DELTA_SPARK_VERSION}/delta-storage-${DELTA_SPARK_VERSION}.jar"

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY pipeline/ pipeline/
COPY config/ config/

CMD ["python", "pipeline/run_all.py"]
