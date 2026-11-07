#!/bin/sh
set -eu

BOOTSTRAP="${BOOTSTRAP:-kafka:19092}"
TOPICS_FILE="${TOPICS_FILE:-/opt/cz/topics.conf}"
REPLICATION="${REPLICATION_FACTOR:-1}"
KAFKA_BIN=/opt/kafka/bin

grep -v '^[[:space:]]*#' "$TOPICS_FILE" | grep -v '^[[:space:]]*$' |
while read -r name partitions retention_ms retention_bytes policy; do
  echo "ensuring topic $name"
  "$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" \
    --create --if-not-exists \
    --topic "$name" \
    --partitions "$partitions" \
    --replication-factor "$REPLICATION"

  # --create leaves existing topics untouched, so configs are applied separately.
  # This keeps topics.conf the source of truth on every start.
  "$KAFKA_BIN/kafka-configs.sh" --bootstrap-server "$BOOTSTRAP" \
    --alter --entity-type topics --entity-name "$name" \
    --add-config "retention.ms=$retention_ms,retention.bytes=$retention_bytes,cleanup.policy=$policy"
done

"$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --list