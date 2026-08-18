# Bronze writer image: Spark + Iceberg + Nessie + S3A(MinIO) + Kafka jars
# baked in at build time, not fetched via --packages on every container
# restart (this service runs with `restart: unless-stopped`).
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
# Build context is the repo root (see docker-compose.yml's build.context)
# so this Dockerfile can also reach infrastructure/iceberg/bronze_ddl.sql.
COPY streaming/pyspark/spark_session.py streaming/pyspark/debezium_envelope.py streaming/pyspark/bronze_writer.py ./
COPY infrastructure/iceberg/bronze_ddl.sql /opt/spark-app/bronze_ddl.sql

USER 185

# --driver-memory sized to fit within the compose service's 2560m mem_limit,
# leaving headroom for JVM/off-heap overhead outside the heap itself.
ENTRYPOINT ["/opt/spark/bin/spark-submit", "--master", "local[*]", "--driver-memory", "1536m", "/opt/spark-app/bronze_writer.py"]
