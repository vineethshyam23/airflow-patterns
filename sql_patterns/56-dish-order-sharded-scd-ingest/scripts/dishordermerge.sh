#!/usr/bin/env bash
# Merge per-shard CSV fragments into one object per table in the raw zone.
#
# For each sharded table:
#   1. gsutil cat shard fragments → local /tmp/{table}.csv (header once)
#   2. gsutil cp to gs://{raw_bucket}/foodorder/{table}/{ds}/{table}.csv
#
# Master tables are NOT merged here — the DAG copies them via GCSToGCS
# from the export bucket into the same raw-zone layout.

set -euo pipefail

EXPORT_BUCKET="${EXPORT_BUCKET:-db-export-food-order-prod}"
RAW_BUCKET="${RAW_BUCKET:-dwh-rawzone}"
DS="${DS:-$(date -u +%Y-%m-%d)}"
WORK="${WORK:-/tmp/foodorder-merge}"

SHARDED_TABLES=(
  orders order_menus order_totals customers menus menu_categories
  locations payments payment_logs reservations statuses status_history
)

mkdir -p "${WORK}"

for table in "${SHARDED_TABLES[@]}"; do
  out="${WORK}/${table}.csv"
  : > "${out}"
  echo "merge ${table}" >&2
  # Production used gsutil -m cat over _shards/${DS}/**/${table}.csv,
  # keeping the first header and stripping subsequent ones.
  # Example:
  #   first=1
  #   while read -r frag; do
  #     if [[ ${first} -eq 1 ]]; then cat "${frag}" >> "${out}"; first=0
  #     else tail -n +2 "${frag}" >> "${out}"; fi
  #   done < <(gsutil ls "gs://${EXPORT_BUCKET}/_shards/${DS}/**/${table}.csv")
  dest="gs://${RAW_BUCKET}/foodorder/${table}/${DS}/${table}.csv"
  echo "  upload → ${dest}" >&2
  # gsutil cp "${out}" "${dest}"
done
