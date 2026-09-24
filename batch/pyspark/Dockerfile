# Gold-builder image: same Iceberg/Nessie/S3A JAR set as the Bronze/Silver/
# data-quality images (streaming/pyspark/Dockerfile), minus Kafka jars --
# this is a one-shot batch job (Silver -> Gold aggregation via plain Spark
# SQL, no dbt -- see docs/decisions/0007-plain-pyspark-gold-not-dbt.md),
# not a long-running streaming consumer.
#
# No pip-installed pyspark and no extra Python dependencies at all -- the
# base image's own bundled PySpark (matching the JARs on $SPARK_HOME/jars)
# is used via spark-submit, same as bronze_writer.py/silver_writer.py.
FROM apache/spark:3.5.9-python3

USER root

ARG ICEBERG_VERSION=1.6.1
ARG NESSIE_VERSION=0.99.0
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_BUNDLE_VERSION=1.12.367

RUN set -eux; \
    JARS_DIR="${SPARK_HOME}/jars"; \
    fetch() { curl -fsSL -o "${JARS_DIR}/$(basename "$1")" "$1"; }; \
    fetch "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar"; \
    fetch "https://repo1.maven.org/maven2/org/projectnessie/nessie-integrations/nessie-spark-extensions-3.5_2.12/${NESSIE_VERSION}/nessie-spark-extensions-3.5_2.12-${NESSIE_VERSION}.jar"; \
    fetch "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar"; \
    fetch "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_BUNDLE_VERSION}/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar"

WORKDIR /opt/spark-app
# Build context is the repo root (see docker-compose.yml's build.context),
# matching streaming/pyspark/Dockerfile's convention. spark_session.py is
# Bronze/Silver's own module -- copied in here too (rather than shared via
# a volume) since each Docker image in this project is self-contained.
COPY streaming/pyspark/spark_session.py /opt/spark-app/spark_session.py
COPY infrastructure/iceberg/gold_ddl.sql /opt/spark-app/gold_ddl.sql
COPY batch/pyspark/gold_builder.py batch/pyspark/gold_tests.py /opt/spark-app/

USER 185

# CMD (not baked into ENTRYPOINT) so `docker compose run --rm gold-builder
# /opt/spark-app/gold_tests.py` can swap in the test script while still
# going through spark-submit with the same --master flag.
ENTRYPOINT ["/opt/spark/bin/spark-submit", "--master", "local[*]"]
CMD ["/opt/spark-app/gold_builder.py"]
