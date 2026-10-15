#!/usr/bin/env bash
# Build tenant→shard mapping CSV from the master food-ordering MySQL DB.
#
# Exports prod.ti_clients JOIN prod.ti_server_instances into
# foodorder_dbs.csv as: db_name,instance_ip
#
# Production used `gcloud sql export csv` against the master instance.
# This stub documents the contract the DAG expects; wire real gcloud
# flags + project/instance names in your Composer environment.

set -euo pipefail

DATA_ROOT="${DATA_ROOT:-/home/airflow/gcs/data/food-order}"
EXPORT_BUCKET="${EXPORT_BUCKET:-db-export-food-order-prod}"
SOURCE_PROJECT="${SOURCE_PROJECT:-food-order-prod}"
MASTER_INSTANCE="${MASTER_INSTANCE:-db-prod-mysql-order-master}"
OUT_CSV="${DATA_ROOT}/foodorder_dbs.csv"

mkdir -p "${DATA_ROOT}"

# SELECT that lands the mapping. Hashes are not needed here — this file
# is only used to filter which tenant DBs live on each shard IP.
QUERY="SELECT c.database_name, s.private_ip
FROM prod.ti_clients c
JOIN prod.ti_server_instances s ON c.server_instance_id = s.id
WHERE c.is_active = 1"

# Example (Composer SA needs cloudsql.instances.export + GCS write):
# gcloud sql export csv "${MASTER_INSTANCE}" \
#   "gs://${EXPORT_BUCKET}/_meta/foodorder_dbs.csv" \
#   --database=prod \
#   --project="${SOURCE_PROJECT}" \
#   --query="${QUERY}"
# gsutil cp "gs://${EXPORT_BUCKET}/_meta/foodorder_dbs.csv" "${OUT_CSV}"

echo "getdbs: write tenant→shard map to ${OUT_CSV}" >&2
# Placeholder empty header so local dry-runs do not break grep filters.
printf 'database_name,private_ip\n' > "${OUT_CSV}"
