# Pattern 58: BigQuery product API refined zone + AlloyDB sync

Composer DAG that WRITE_TRUNCATEs six product-suite API tables in
BigQuery (plus a wholesale→establishment map), then incrementally
syncs one market CRM-activity dashboard into AlloyDB with
`ON CONFLICT (unique_key) DO NOTHING`.

Distinct from API Gateway / Apigee work in `api-integrations`: this
pattern stops at warehouse dual-store prep. Distinct from Cloud SQL
land patterns (#27, #39, #54, #56): direction here is BQ → Postgres
serving, not OLTP → BQ.

Source (read-only):
- `dags/etl_api_alloydb.py`
- `dags/horeca_digital/DISH_api_query.py`

## Files

| File | Role |
|------|------|
| `dag_bq_alloydb_api_sync.py` | Parallel BQ truncate + AlloyDB incremental sync |
| `product_api_queries.py` | SQL builders for website / reservation / order / establishment / POS / dashboard |
| `BUSINESS_CASE.md` | Why dual-store, why only one table syncs |
| `ARCHITECTURE.md` | Components + Mermaid diagram |
| `DATA_FLOW.md` | Schedule, idempotency, failure modes |

## Quick start

```bash
python -c "import ast; ast.parse(open('product_api_queries.py').read())"
python -c "import ast; ast.parse(open('dag_bq_alloydb_api_sync.py').read())"
```

To run for real you need Airflow Variables `env`, `dwh_project_id`
(and `_dev`), `alloydb_prod_creds` / `alloydb_dev_creds` (JSON for
`psycopg2.connect`), BigQuery refined sources, and an AlloyDB schema
`api_refined.api_dashboard_market` with a unique constraint on
`unique_key`. This folder is a sanitized reference, not a deploy.

## Sanitization notes

- GCP projects `hd-dwh-stream-*` → `dwh_project` / `dwh_project_dev`
- Datasets `dwh_refined` / `dwh_api_refined` → `refined` / `api_refined`
- Product brand / DISH naming → product suite / generic table names
- Metro / MCC identifiers → `wholesale_*`
- Real demo establishment / account UUIDs → placeholder UUIDs
- Emails → `dataops@example.com`; owner → `data-platform`
- Commented `__main__` block with plaintext AlloyDB password removed
- Package import `horeca_digital.DISH_api_query` → local `product_api_queries`
- Cursor-only context manager → `(conn, cursor)` with explicit close
- Added `max_active_runs=1` and AlloyDB `execution_timeout`

## Category

`utilities/58-bq-alloydb-api-sync/`
