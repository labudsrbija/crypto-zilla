#!/bin/sh
# Usage: cz-submit <job> [job args]
# Submits a cryptozilla job to the cluster in client mode (the only mode PySpark supports on a
# standalone cluster). The driver runs in this container, so executors must be able to reach it.
set -eu

job="$1"
shift

exec spark-submit \
  --master "${SPARK_MASTER_URL:-spark://spark-master:7077}" \
  --deploy-mode client \
  --name "cz-$job" \
  --conf "spark.driver.host=${SPARK_DRIVER_HOST:-$(hostname)}" \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.driver.port=7078 \
  --conf spark.blockManager.port=7079 \
  --conf "spark.driver.memory=${SPARK_DRIVER_MEMORY:-1g}" \
  --conf "spark.executor.memory=${SPARK_EXECUTOR_MEMORY:-1g}" \
  --conf "spark.executor.cores=${SPARK_EXECUTOR_CORES:-1}" \
  --conf "spark.cores.max=${SPARK_CORES_MAX:-2}" \
  "$CZ_JOBS_ENTRY" "$job" "$@"