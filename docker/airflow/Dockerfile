FROM apache/airflow:2.9.3-python3.12

USER root
# PySpark needs a JVM even in local[*] mode; its launch script also shells
# out to `ps`, which isn't in the base image (procps).
RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jdk-headless procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/default-java

USER airflow
COPY requirements-airflow.txt /requirements-airflow.txt
# Installing against Airflow's own constraints file keeps pip from upgrading
# apache-airflow itself while resolving the extra deps below.
RUN pip install --no-cache-dir \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.12.txt" \
      -r /requirements-airflow.txt

# Iceberg table writes need these on the classpath:
#   - iceberg-spark-runtime: Iceberg's Spark integration
#   - iceberg-aws-bundle: Iceberg's own S3FileIO (AWS SDK v2), independent of
#     Hadoop's s3a/hadoop-aws
#   - postgresql (JDBC driver): Iceberg's JdbcCatalog, pointed at the same
#     Postgres already running for Airflow's metadata DB, tracks table/
#     namespace pointers as plain rows. Catalog choice matters here: Iceberg's
#     "hadoop" catalog type was tried first and fails against S3 with
#     `UnsupportedFileSystemException: No FileSystem for scheme "s3"` — some
#     of its catalog-level bookkeeping goes through Hadoop's classic
#     FileSystem API (needing hadoop-aws) even though data/metadata file I/O
#     otherwise goes through Iceberg's own S3FileIO. JdbcCatalog never touches
#     Hadoop's FileSystem API at all, avoiding that dependency entirely — and
#     it's also not Iceberg's GlueCatalog, so Glue registration still stays in
#     Python via moto (see dags/sec_13f_pipeline.py) rather than needing to
#     intercept a JVM-side Glue client moto could never see.
ARG ICEBERG_VERSION=1.6.1
ARG POSTGRESQL_JDBC_VERSION=42.7.4
RUN PYSPARK_JARS_DIR=$(python -c "import pyspark, os; print(os.path.join(os.path.dirname(pyspark.__file__), 'jars'))") \
    && curl -fsSL -o "$PYSPARK_JARS_DIR/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar" \
      "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar" \
    && curl -fsSL -o "$PYSPARK_JARS_DIR/iceberg-aws-bundle-${ICEBERG_VERSION}.jar" \
      "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar" \
    && curl -fsSL -o "$PYSPARK_JARS_DIR/postgresql-${POSTGRESQL_JDBC_VERSION}.jar" \
      "https://repo1.maven.org/maven2/org/postgresql/postgresql/${POSTGRESQL_JDBC_VERSION}/postgresql-${POSTGRESQL_JDBC_VERSION}.jar"
