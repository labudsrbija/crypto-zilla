#!/bin/sh
set -eu

: "${MINIO_ROOT_USER:?}" "${MINIO_ROOT_PASSWORD:?}"
: "${WAREHOUSE_BUCKET:?}" "${CATALOG_S3_ACCESS_KEY:?}" "${CATALOG_S3_SECRET_KEY:?}"

until mc alias set cz http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null 2>&1; do
  echo "waiting for minio"
  sleep 2
done

mc mb --ignore-existing "cz/$WAREHOUSE_BUCKET"

# The catalog gets its own user, limited to the warehouse bucket, instead of root keys.
mc admin policy create cz warehouse-rw /opt/cz/warehouse-policy.json
mc admin user add cz "$CATALOG_S3_ACCESS_KEY" "$CATALOG_S3_SECRET_KEY"
mc admin policy attach cz warehouse-rw --user "$CATALOG_S3_ACCESS_KEY" 2>/dev/null \
  || echo "policy already attached"

echo "minio ready: bucket $WAREHOUSE_BUCKET, user $CATALOG_S3_ACCESS_KEY"