#!/usr/bin/env bash
# Per-shard export of tenant tables from one food-ordering Cloud SQL instance.
#
# Args: <instance_name> <private_ip>
# Reads foodorder_dbs_<instance>.csv (one tenant DB name per line) produced
# by the DAG's getdbs_<instance> grep filter, then runs gcloud sql export
# for each sharded table × tenant DB.
#
# Production slept ~10s between table exports to keep Cloud SQL Admin
# from 409-ing under parallel shard fan-out.

set -euo pipefail

INSTANCE="${1:?instance name required}"
IP="${2:?private ip required}"
DATA_ROOT="${DATA_ROOT:-/home/airflow/gcs/data/food-order}"
EXPORT_BUCKET="${EXPORT_BUCKET:-db-export-food-order-prod}"
SOURCE_PROJECT="${SOURCE_PROJECT:-food-order-prod}"
DS="${DS:-$(date -u +%Y-%m-%d)}"
DB_LIST="${DATA_ROOT}/foodorder_dbs_${INSTANCE}.csv"

SHARDED_TABLES=(
  orders order_menus order_totals customers menus menu_categories
  locations payments payment_logs reservations statuses status_history
)

if [[ ! -f "${DB_LIST}" ]]; then
  echo "missing tenant list: ${DB_LIST}" >&2
  exit 1
fi

echo "shard export instance=${INSTANCE} ip=${IP}" >&2

while IFS= read -r db_name || [[ -n "${db_name}" ]]; do
  [[ -z "${db_name}" || "${db_name}" == database_name* ]] && continue
  for table in "${SHARDED_TABLES[@]}"; do
    # Fragment path: shard CSVs land under a working prefix; merge step
    # concatenates into foodorder/{table}/{ds}/{table}.csv in the raw zone.
    uri="gs://${EXPORT_BUCKET}/_shards/${DS}/${INSTANCE}/${db_name}/${table}.csv"
    echo "  export ${db_name}.${table} → ${uri}" >&2
    # gcloud sql export csv "${INSTANCE}" "${uri}" \
    #   --database="${db_name}" --project="${SOURCE_PROJECT}" --query="..."
    sleep 1
  done
done < "${DB_LIST}"
