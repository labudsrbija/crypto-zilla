#!/bin/sh
# Resolves Maven packages with Spark's own resolver, then copies into $SPARK_HOME/jars only
# the jars Spark doesn't already ship, so we never end up with two versions of one library.
set -eu

PACKAGES="$1"
IVY_DIR=/tmp/ivy
JARS_DIR="$SPARK_HOME/jars"

echo "pass" > /tmp/noop.py
spark-submit --master 'local[1]' --conf "spark.jars.ivy=$IVY_DIR" --packages "$PACKAGES" /tmp/noop.py

for jar in "$IVY_DIR"/jars/*.jar; do
  # ivy names jars <group>_<artifact>-<version>.jar
  artifact=$(basename "$jar" | sed -E 's/^[^_]+_//; s/-[0-9][^-]*(-[0-9]+)?\.jar$//')
  if ls "$JARS_DIR/$artifact"-[0-9]*.jar >/dev/null 2>&1; then
    echo "skip $artifact (provided by spark)"
  else
    cp "$jar" "$JARS_DIR/"
    echo "add  $(basename "$jar")"
  fi
done

rm -rf "$IVY_DIR" /tmp/noop.py