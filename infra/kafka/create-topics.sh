#!/bin/sh
set -eu

BOOTSTRAP="${BOOTSTRAP:-kafka:19092}"
TOPICS_FILE="${TOPICS_FILE:-/opt/cz/topics.conf}"
REPLICATION="${REPLICATION_FACTOR:-1}"
KAFKA_BIN=/opt/kafka/bin

grep -v '^[[:space:]]*#' "$TOPICS_FILE" | grep -v '^[[:space:]]*$' | while read -r name partitions retention policy; do
  echo "ensuring topic $name (partitions=$partitions retention.ms=$retention cleanup.policy=$policy)"
  "$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" \
    --create --if-not-exists \
    --topic "$name" \
    --partitions "$partitions" \
    --replication-factor "$REPLICATION" \
    --config "retention.ms=$retention" \
    --config "cleanup.policy=$policy"
done

"$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --list
