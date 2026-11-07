#!/bin/sh
set -eu

: "${CATALOG_WAREHOUSE:?}" "${WAREHOUSE_BUCKET:?}"
: "${CATALOG_S3_ACCESS_KEY:?}" "${CATALOG_S3_SECRET_KEY:?}"

API="${LAKEKEEPER_URL:-http://lakekeeper:8181}/management/v1"
PROJECT="00000000-0000-0000-0000-000000000000"
S3_ENDPOINT="${S3_ENDPOINT:-http://minio:9000}"
S3_REGION="${S3_REGION:-local-01}"

post() {
  curl -s -o /tmp/body -w '%{http_code}' -X POST "$API/$1" \
    -H 'Content-Type: application/json' -H "x-project-id: $PROJECT" --data "$2"
}

until code=$(post bootstrap '{"accept-terms-of-use": true}') && [ "$code" != "000" ]; do
  echo "waiting for lakekeeper"
  sleep 2
done

case "$code" in
  2*) echo "lakekeeper bootstrapped" ;;
  *)
    if grep -qi "already" /tmp/body; then
      echo "lakekeeper already bootstrapped"
    else
      echo "bootstrap failed ($code):" && cat /tmp/body && exit 1
    fi
    ;;
esac

if curl -sf "$API/warehouse" -H "x-project-id: $PROJECT" | grep -q "\"$CATALOG_WAREHOUSE\""; then
  echo "warehouse $CATALOG_WAREHOUSE exists"
  exit 0
fi

# sts-enabled: Lakekeeper hands Spark short-lived credentials per table (vended credentials),
# so engines never hold the storage keys.
code=$(post warehouse "$(cat <<EOF
{
  "warehouse-name": "$CATALOG_WAREHOUSE",
  "storage-profile": {
    "type": "s3",
    "bucket": "$WAREHOUSE_BUCKET",
    "key-prefix": "lakekeeper",
    "region": "$S3_REGION",
    "endpoint": "$S3_ENDPOINT",
    "path-style-access": true,
    "flavor": "s3-compat",
    "sts-enabled": true
  },
  "storage-credential": {
    "type": "s3",
    "credential-type": "access-key",
    "aws-access-key-id": "$CATALOG_S3_ACCESS_KEY",
    "aws-secret-access-key": "$CATALOG_S3_SECRET_KEY"
  },
  "delete-profile": {"type": "hard"}
}
EOF
)")

case "$code" in
  2*) echo "warehouse $CATALOG_WAREHOUSE created" ;;
  *) echo "warehouse creation failed ($code):" && cat /tmp/body && exit 1 ;;
esac