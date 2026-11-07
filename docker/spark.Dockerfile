# One image for the Spark master, workers and drivers. Spark comes from the pyspark wheel so
# the driver and the executors run the exact same Python as our package and tests.
FROM python:3.12-slim-trixie

ARG ICEBERG_VERSION=1.12.0
ARG SPARK_VERSION=4.1.4

RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-21-jre-headless procps tini \
 && rm -rf /var/lib/apt/lists/* \
 && ln -s "$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")" /opt/java

COPY --from=ghcr.io/astral-sh/uv:0.11.32 /uv /usr/local/bin/uv

ENV JAVA_HOME=/opt/java \
    VIRTUAL_ENV=/opt/venv \
    PATH="/opt/venv/bin:$PATH" \
    UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy UV_PYTHON_DOWNLOADS=never \
    PYTHONUNBUFFERED=1

RUN uv venv /opt/venv && uv pip install --no-cache "pyspark==${SPARK_VERSION}"

ENV SPARK_HOME=/opt/venv/lib/python3.12/site-packages/pyspark \
    PYSPARK_PYTHON=/opt/venv/bin/python \
    PYSPARK_DRIVER_PYTHON=/opt/venv/bin/python

COPY --chmod=0755 infra/spark/install-jars.sh /usr/local/bin/install-jars
RUN install-jars "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:${ICEBERG_VERSION},org.apache.iceberg:iceberg-aws-bundle:${ICEBERG_VERSION},org.apache.spark:spark-sql-kafka-0-10_2.13:${SPARK_VERSION}"

COPY infra/spark/spark-defaults.conf ${SPARK_HOME}/conf/spark-defaults.conf
COPY --chmod=0755 infra/spark/submit.sh /usr/local/bin/cz-submit

# pyspark and the jars live in the layers above, so a code change only rebuilds from here
COPY pyproject.toml README.md /src/
COPY src /src/src
RUN uv pip install --no-cache "/src[spark]" && rm -rf /src

ENV CZ_JOBS_ENTRY=/opt/venv/lib/python3.12/site-packages/cryptozilla/jobs/__main__.py

RUN useradd --create-home --uid 10001 spark \
 && mkdir -p /opt/cz/checkpoints /opt/spark-work \
 && chown -R spark /opt/cz /opt/spark-work

USER spark
WORKDIR /home/spark
ENTRYPOINT ["tini", "--"]