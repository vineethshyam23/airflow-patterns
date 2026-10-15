# Pattern 54: Reservation Tool incremental Cloud SQL export

Composer DAG that lands a restaurant-reservation OLTP schema from
Cloud SQL MySQL into BigQuery staging, then hands off to one dbt
Cloud job. The reusable piece is the **id-watermark + Sunday full
sync** orchestration: daily exports for append-heavy tables become
``WHERE id > MAX(id)`` instead of full dumps, while composite-key
tables stay on WRITE_TRUNCATE.

Distinct from pattern 39 (Hydra weekly full dump): Hydra has no
watermark and truncates every table every run. Distinct from pattern
27 (Offer Tool SCD ingest): that path merges SCD Type 2 inside the
DAG; here historization and PII masking stay in dbt.

Source (read-only):
- `dags/etl_reservationtool_v2.py`
- `dags/horeca_digital/rt_table_config.py`
- `dags/horeca_digital/operators/cloudsql_retry_operator.py`

## Files

| File | Role |
|------|------|
| `dag_reservationtool_v2.py` | Watermark task + serial export → parallel load → dbt |
| `rt_table_config.py` | INCREMENTAL / FULL_LOAD catalog + CSV-safe SELECT builder |
| `cloudsql_export_operator.py` | 409 retry + backup-window aware exporter (same family as #39) |
| `BUSINESS_CASE.md` | Why watermark + weekly baseline beat nightly full dump |
| `ARCHITECTURE.md` | Components + Mermaid diagram |
| `DATA_FLOW.md` | Run order, CSV contract, failure modes |

## Quick start

```bash
python -c "import ast; ast.parse(open('cloudsql_export_operator.py').read())"
python -c "import ast; ast.parse(open('rt_table_config.py').read())"
python -c "import ast; ast.parse(open('dag_reservationtool_v2.py').read())"
python -c "from rt_table_config import build_select_query; print(build_select_query('reservations')[:120])"
```

To run for real you need Cloud SQL Admin export IAM on the source
instance, GCS write + BQ load on the raw bucket, schema JSON objects
under `schema_json/rt_*.json`, and Variables for project / instance /
bucket / dbt job id. This folder is a sanitized reference, not a
deploy.

## Sanitization notes

- GCP projects (`hd-dwh-stream-1`, product reservation project id) →
  Variables + `dwh_project` / `reservation_project` placeholders
- Cloud SQL instance / database names → Variables
- Raw bucket `hd-digital-dp-rawzone` → `dwh-rawzone` / Variable
- Hardcoded dbt job id → Variable `rt_dbt_job_id` (EmptyOperator stub
  when unset)
- Owner / author names and real emails removed
- Table catalog trimmed from ~50 production tables to 9 representative
  specs (5 incremental + 4 full-load)
- Password / reset-token / Facebook access token remain in
  `EXCLUDE_COLUMNS` and never appear in `_TABLE_COLUMNS`
- Optional dbt provider stub; `max_active_runs=1` preserved
- Inline imports in the original watermark helper and operator moved
  to module top

## Category

`custom_operators/54-reservation-tool-incremental-export/`
