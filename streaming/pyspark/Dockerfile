# Bronze/Silver/Analytics writer image: Spark + Iceberg + Nessie + S3A(MinIO)
# + Kafka jars baked in at build time, not fetched via --packages on every
# container restart (these services run with `restart: unless-stopped`).
FROM apache/spark:3.5.9-python3

USER root

ARG ICEBERG_VERSION=1.6.1
ARG NESSIE_VERSION=0.99.0
ARG SPARK_KAFKA_VERSION=3.5.9
ARG KAFKA_CLIENTS_VERSION=3.8.0
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_BUNDLE_VERSION=1.12.367
ARG COMMONS_POOL2_VERSION=2.11.1

RUN set -eux; \
    JARS_DIR="${SPARK_HOME}/jars"; \
    fetch() { curl -fsSL -o "${JARS_DIR}/$(basename "$1")" "$1"; }; \
    fetch "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar"; \
    fetch "https://repo1.maven.org/maven2/org/projectnessie/nessie-integrations/nessie-spark-extensions-3.5_2.12/${NESSIE_VERSION}/nessie-spark-extensions-3.5_2.12-${NESSIE_VERSION}.jar"; \
    fetch "https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/${SPARK_KAFKA_VERSION}/spark-sql-kafka-0-10_2.12-${SPARK_KAFKA_VERSION}.jar"; \
    fetch "https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/${SPARK_KAFKA_VERSION}/spark-token-provider-kafka-0-10_2.12-${SPARK_KAFKA_VERSION}.jar"; \
    fetch "https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar"; \
    fetch "https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/${COMMONS_POOL2_VERSION}/commons-pool2-${COMMONS_POOL2_VERSION}.jar"; \
    fetch "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar"; \
    fetch "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_BUNDLE_VERSION}/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar"

WORKDIR /opt/spark-app
# Build context is the repo root (see docker-compose.yml's build.context) so
# this Dockerfile can also reach infrastructure/iceberg/*.sql. One image
# backs the Bronze, Silver, and Analytics writers (spark-bronze-writer /
# spark-silver-writer / spark-analytics-writer compose services) -- they
# share the same JAR set, and each service selects its entrypoint script via
# `command:` (see below).
COPY streaming/pyspark/spark_session.py streaming/pyspark/debezium_envelope.py streaming/pyspark/bronze_writer.py \
     streaming/pyspark/silver_schemas.py streaming/pyspark/silver_tables.py streaming/pyspark/silver_transform.py \
     streaming/pyspark/scd2.py streaming/pyspark/silver_writer.py streaming/pyspark/schema_evolution.py \
     streaming/pyspark/analytics_config.py streaming/pyspark/analytics_transform.py \
     streaming/pyspark/analytics_writer.py ./
COPY infrastructure/iceberg/bronze_ddl.sql /opt/spark-app/bronze_ddl.sql
COPY infrastructure/iceberg/silver_ddl.sql /opt/spark-app/silver_ddl.sql
COPY infrastructure/iceberg/gold_ddl.sql /opt/spark-app/gold_ddl.sql
COPY infrastructure/iceberg/analytics_ddl.sql /opt/spark-app/analytics_ddl.sql

USER 185

# --driver-memory sized to fit within the compose services' 2560m mem_limit,
# leaving headroom for JVM/off-heap overhead outside the heap itself. The
# target script (bronze_writer.py, silver_writer.py, or analytics_writer.py)
# is supplied by each compose service's `command:`, not hardcoded here.
#
# --master local[2] (not local[*]): this image backs THREE concurrent
# long-running writers (spark-bronze-writer/-silver-writer/-analytics-writer),
# each running many parallel Structured Streaming queries in one JVM
# (Bronze: 7, Silver: 7, Analytics: 6). local[*] makes each JVM claim every
# host core for its own thread pool -- three JVMs doing that simultaneously
# oversubscribes the same cores 3x over, which was observed to cause severe
# enough scheduling/GC contention to sever the py4j bridge between the
# Python driver and the JVM ("Received empty command", every in-flight job
# failing at once after a multi-second stall) and crash-loop the writer.
# local[4] per writer (12 threads total across the 3) still left the writers
# crash-looping on a 16-core host -- combined measured CPU across just the 3
# writers was ~13.6 cores even at that cap, since local[N] only bounds Spark
# TASK threads, not JVM overhead (GC/Netty/JIT/py4j threads), which adds
# meaningfully on top. local[2] x 3 writers = 6 task threads, leaving real
# headroom for that overhead plus Kafka/Kafka Connect/Postgres/Nessie/MinIO.
# Tune alongside mem_limit below if this ever runs on a different-sized host.
ENTRYPOINT ["/opt/spark/bin/spark-submit", "--master", "local[2]", "--driver-memory", "1536m"]
CMD ["/opt/spark-app/bronze_writer.py"]
