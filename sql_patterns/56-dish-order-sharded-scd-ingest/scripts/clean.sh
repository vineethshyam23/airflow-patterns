#!/usr/bin/env bash
# Remove local CSV working files after the SCD load barrier completes.
# Runs with trigger_rule=ALL_DONE so cleanup still happens on partial failure.

set -euo pipefail

DATA_ROOT="${DATA_ROOT:-/home/airflow/gcs/data/food-order}"
WORK="${WORK:-/tmp/foodorder-merge}"

echo "clean local food-order CSV scratch under ${DATA_ROOT} and ${WORK}" >&2
rm -f "${DATA_ROOT}"/foodorder_dbs_*.csv 2>/dev/null || true
rm -rf "${WORK}" 2>/dev/null || true
