# Data-quality gate image: same Iceberg/Nessie/S3A JAR set as the Bronze/
# Silver/dbt images (streaming/pyspark/Dockerfile), minus Kafka jars.
#
# Deliberately NOT pip-installing pyspark -- like bronze_writer.py/
# silver_writer.py, this image's entrypoint runs everything via
# spark-submit (see ENTRYPOINT/CMD below), which injects the base image's
# own bundled PySpark (matching the JARs on $SPARK_HOME/jars) onto sys.path
# at runtime. A separately pip-installed pyspark distribution would risk a
# version mismatch against those JARs. great_expectations lazy-imports
# pyspark only when its Spark execution engine is actually exercised (see
# data_quality/ge_helpers.py), so this works without declaring pyspark as a
# pip dependency at all.
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

COPY data_quality/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

WORKDIR /opt/spark-app
# Build context is the repo root (see docker-compose.yml's build.context),
# matching streaming/pyspark/Dockerfile's convention. spark_session.py is
# Bronze/Silver's own module -- copied in here too (rather than shared via
# a volume) since each Docker image in this project is self-contained.
COPY streaming/pyspark/spark_session.py /opt/spark-app/spark_session.py
COPY scripts/spark_query.py /opt/spark-app/spark_query.py
COPY data_quality/runner.py data_quality/severity.py data_quality/ge_helpers.py /opt/spark-app/
COPY data_quality/checks/ /opt/spark-app/checks/
COPY data_quality/suites/ /opt/spark-app/suites/

USER 185

# CMD (not baked into ENTRYPOINT) so `docker compose run --rm dq
# /opt/spark-app/spark_query.py --sql "..."` can swap in a different script
# while still going through spark-submit with the same --master flag.
ENTRYPOINT ["/opt/spark/bin/spark-submit", "--master", "local[*]"]
CMD ["/opt/spark-app/runner.py"]
