# Single image used by every Python service (Spark master/worker, Dagster, dashboard)
# -> driver and executors always share the exact same Python / Spark / jar versions.
FROM python:3.11-slim-bookworm

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl procps tini \
    && rm -rf /var/lib/apt/lists/* \
    # architecture-independent JAVA_HOME (amd64 / arm64)
    && ln -sfn "$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")" /opt/java

ENV JAVA_HOME=/opt/java

COPY requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt

# Spark ships with pyspark; add the S3 (s3a) and Delta Lake jars to its classpath.
# Versions must match Spark 3.5 / Hadoop 3.3.4 / Scala 2.12.
ENV SPARK_HOME=/usr/local/lib/python3.11/site-packages/pyspark
ENV PATH="${SPARK_HOME}/bin:${PATH}" \
    PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3
ARG MAVEN=https://repo1.maven.org/maven2
RUN cd "${SPARK_HOME}/jars" \
    && curl -fsSLO ${MAVEN}/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
    && curl -fsSLO ${MAVEN}/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
    && curl -fsSLO ${MAVEN}/io/delta/delta-spark_2.12/3.3.2/delta-spark_2.12-3.3.2.jar \
    && curl -fsSLO ${MAVEN}/io/delta/delta-storage/3.3.2/delta-storage-3.3.2.jar

WORKDIR /opt/project
COPY pipeline ./pipeline
COPY dashboard ./dashboard
COPY tests ./tests
COPY scripts ./scripts
COPY data/reference ./data/reference
COPY pytest.ini ./
COPY dagster_home /opt/dagster/dagster_home

ENV PYTHONPATH=/opt/project \
    DAGSTER_HOME=/opt/dagster/dagster_home

ENTRYPOINT ["/usr/bin/tini", "--"]
