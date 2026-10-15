# Pattern 39: Hydra Cloud SQL weekly full export (v2)

Composer DAG that dumps website CMS tables from Cloud SQL MySQL to
GCS as CSV, loads them into BigQuery staging (`hyd_v2_*`), then runs
one dbt Cloud job. The reusable piece is a schedule-aware Cloud SQL
export operator that survives `operationInProgress` collisions and
backup windows.

Distinct from pattern 27 (Offer Tool SCD Type 2 ingest): that path
watermarks and merges. Hydra v2 is intentionally a full REPLACE per
table; SCD / soft-delete history belongs in dbt snapshots.

Source (read-only):
- `dags/etl_hydra_job_v2.py`
- `dags/horeca_digital/hydra_raw_export_queries.py`
- `dags/horeca_digital/operators/cloudsql_retry_operator.py`

## Files

| File | Role |
|------|------|
| `dag_hydra_v2.py` | Sequential export→load TaskGroup + dbt fan-in |
| `cloudsql_export_operator.py` | 409 retry + backup-window aware exporter |
| `hydra_export_queries.py` | CSV-safe SELECT builders, type map, schema/dbt CLI |
| `BUSINESS_CASE.md` | Why full dump + dbt snapshots beat legacy hyd_* |
| `ARCHITECTURE.md` | Components + Mermaid diagram |
| `DATA_FLOW.md` | Run order, CSV pitfalls, failure modes |

## Quick start

```bash
python -c "import ast; ast.parse(open('cloudsql_export_operator.py').read())"
python -c "import ast; ast.parse(open('hydra_export_queries.py').read())"
python -c "import ast; ast.parse(open('dag_hydra_v2.py').read())"
python hydra_export_queries.py schemas /tmp/hyd_v2_schemas
```

To run for real you need Cloud SQL Admin export IAM on the source
instance, GCS write + BQ load on the raw bucket, schema JSON objects
under `schema_json/hyd_v2_*.json`, Variables for instance / bucket /
dbt job id, and dbt models tagged `hydra_v2`. This folder is a
sanitized reference, not a deploy.

## Sanitization notes

- GCP projects `hd-dwh-stream-1` / website project ids → Variables +
  `dwh_project` / `website_project` placeholders
- Cloud SQL instance / database names → Variables
- Raw bucket `hd-digital-dp-rawzone` → `dwh-rawzone` / Variable
- dbt job numeric id → Variable `hydra_v2_dbt_job_id`
- Owner / author names and real emails removed
- Table catalog trimmed from ~80 production tables to 5 representative
  specs (countries, users, establishments, junction, no-PK migration)
- Sensitive / OAuth table exclusion sets kept as empty-guard pattern
- Password columns still listed on `users` export (as in source) but
  stripped from generated dbt staging via `HYDRA_STG_EXCLUDED_COLUMNS`
- Optional dbt provider stub; `max_active_runs=1` preserved
- Fixed inline `datetime` import in the schedule-aware operator

## Category

`custom_operators/39-hydra-cloudsql-weekly-export/`
