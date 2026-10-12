# Pattern 73: User Management Cloud SQL SCD Type 2

Daily Composer land of an identity/access MySQL database into BigQuery
trusted with Type 2 historization. The reusable pieces are **serial
schedule-aware Cloud SQL CSV export**, **semicolon + inline schema
load**, **namespaced surrogate keys at extract**, and a **first-run
bootstrap** before the hash SCD loop.

Distinct from pattern 27 (Offer Tool SCD on a different product DB),
39 (Hydra weekly full dump, no SCD in DAG), 54 (Reservation Tool
id-watermark → dbt), and 56 (multi-shard food-order merge + SCD).

Source (read-only):
- `dags/etl_user_management.py`
- `dags/horeca_digital/operators/cloudsql_retry_operator.py`

## Files

| File | Role |
|------|------|
| `um_table_config.py` | Trimmed schemas + MySQL SELECTs with `UM_` sk + hashes |
| `cloudsql_export_operator.py` | 409 retry + backup-window aware exporter (#39 / #54 family) |
| `dag_user_management_scd.py` | Serial export → dual-bucket → staging → bootstrap → SCD2 |
| `BUSINESS_CASE.md` | Why full daily identity SCD in Composer |
| `ARCHITECTURE.md` | Components + Mermaid diagram |
| `DATA_FLOW.md` | Chain order, CSV/SCD contract, failure modes |

## Quick start

```bash
python -c "import ast; ast.parse(open('cloudsql_export_operator.py').read())"
python -c "import ast; ast.parse(open('um_table_config.py').read())"
python -c "import ast; ast.parse(open('dag_user_management_scd.py').read())"
python -c "from um_table_config import TABLE_NAMES, EXPORT_QUERIES, TABLE_SCHEMAS; assert len(TABLE_NAMES)==8; print(TABLE_NAMES)"
```

Needs Cloud SQL Admin export IAM on the identity instance, GCS write on
the export + raw buckets, and `trusted` / `trusted_staging` datasets.
Wire Variables below. This folder is a sanitized reference, not a deploy.

### Variables

| Variable | Purpose |
|----------|---------|
| `dwh_project_id` | BigQuery project |
| `um_cloudsql_database` | MySQL database name |
| `um_cloudsql_project` | Cloud SQL GCP project |
| `um_cloudsql_instance` | Cloud SQL instance id |
| `um_export_bucket` | Instance export bucket |
| `um_raw_bucket` | DWH raw zone |
| `um_backup_window_start_hour` | Default `3` |
| `um_backup_window_duration_hours` | Default `2` |

## Sanitization notes

- Product / GCP projects (`dish-microservices-prod-*`, `hd-dwh-stream-1`)
  → Variables + `identity-microservices-prod` / `dwh_project`
- Database `dish_um` → `identity_um`; buckets → `db-export-identity-*` /
  `dwh-rawzone`
- Datasets `dwh_trusted*` → `trusted` / `trusted_staging`
- Surrogate prefix `EU_` → `UM_` (namespacing preserved)
- ~28 production tables → 8 representative (users/roles/personas/features/locks)
- Omitted OAuth token tables, REST password columns, session attribute blobs
- Owner / real emails → `data-platform` / `dataops@example.com`;
  `email_on_failure` enabled in the reference (prod had it off)
- Deprecated `airflow.contrib.*` transfers → Google provider operators
- Inline `google.cloud.bigquery` import in bootstrap moved to module top
- Parse-time `date.today()` GCS folders → `{{ ds }}`
- Unused / mistyped `xecution_date` echo template dropped

## Distinct from nearby patterns

| | 27 Offer Tool | 39 / 54 | 56 shards | 73 (this) |
|---|---|---|---|---|
| Source | 1 Offer Tool MySQL | CMS / Reservation | Master + N shards | 1 identity MySQL |
| Export | CloudSQLExportInstance | Admin CSV + retry op | bash gcloud | Admin CSV + retry op |
| CSV | comma-ish | comma | merge | **semicolon** |
| Surrogate | none / product | n/a | n/a | **`UM_` sk at extract** |
| Bootstrap | no | n/a | no | **first-run trusted create** |
| Historization | SCD2 in DAG | dbt / watermark | SCD2 in DAG | SCD2 in DAG |

## Category

`sql_patterns/73-user-management-cloudsql-scd/`
