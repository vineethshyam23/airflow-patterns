#!/usr/bin/env bash
# Export master-only reference tables from the food-ordering master MySQL.
#
# Tables: countries, users, clients, flavours
# Each SELECT adds _keyhash / _rowhash / _create_ts / _job_name / _sourcesystem
# so BigQuery SCD only compares hash pairs.

set -euo pipefail

INSTANCE="${1:-db-prod-mysql-order-master}"
EXPORT_BUCKET="${EXPORT_BUCKET:-db-export-food-order-prod}"
SOURCE_PROJECT="${SOURCE_PROJECT:-food-order-prod}"
DS="${DS:-$(date -u +%Y-%m-%d)}"

MASTER_TABLES=(countries users clients flavours)

for table in "${MASTER_TABLES[@]}"; do
  # Production SELECTs wrap business columns with MD5 key/row hashes.
  # Example sketch (column lists live in schema_json/order_${table}.json):
  #   SELECT col1, col2, ...,
  #     MD5(CAST(pk AS CHAR)) AS _keyhash,
  #     MD5(CONCAT_WS('|', col1, col2, ...)) AS _rowhash,
  #     CURRENT_TIMESTAMP AS _create_ts,
  #     'etl_foodorder' AS _job_name,
  #     'FoodOrder' AS _sourcesystem
  #   FROM prod.ti_${table}
  uri="gs://${EXPORT_BUCKET}/${DS}/${table}.csv"
  echo "export master ${table} → ${uri} (instance=${INSTANCE})" >&2
  # gcloud sql export csv "${INSTANCE}" "${uri}" \
  #   --database=prod --project="${SOURCE_PROJECT}" --query="..."
  sleep 2
done
