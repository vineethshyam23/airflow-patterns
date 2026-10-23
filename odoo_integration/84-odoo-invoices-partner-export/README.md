# Pattern 84: Odoo invoices partner export (FR/ES fingerprint batches)

Daily pipeline that refreshes refined Odoo invoice lines via dbt
Cloud, then ships a full-country Avro feed to a partner event API
for FR and ES. Countries run sequentially; inside each country five
`FARM_FINGERPRINT` batch tasks stream shards in parallel (Composer
pool capped at three concurrent tasks).

Distinct from pattern 08 (WSL wholesale dual export: trusted
intermediate + recommender APPEND + single DE market). This pattern
owns the general invoice-line partner subject with hash partitions
and no ML history sink.

Source (read-only):
- `dags/etl_dana_odoo_invoices_export.py`
- `dags/horeca_digital/dana_odoo_invoices_export.py`
- `dags/horeca_digital/dana_auth.py` (token stagger / retry helpers)

## Files

| File | Role |
|------|------|
| `event_api_auth.py` | Stagger + throttle-aware token retry for parallel batches |
| `odoo_invoices_export.py` | Queries, OAuth, Avro, chunked POST |
| `dag_odoo_invoices_export.py` | Composer DAG: dbt → FR/ES batch fan-out |
| `BUSINESS_CASE.md` | Why this feed, hash batches, and pool tuning |
| `ARCHITECTURE.md` | Components + Mermaid diagram |
| `DATA_FLOW.md` | Run order, idempotency, failure modes |

## Quick start

```bash
python -c "import ast; ast.parse(open('event_api_auth.py').read())"
python -c "import ast; ast.parse(open('odoo_invoices_export.py').read())"
python -c "import ast; ast.parse(open('dag_odoo_invoices_export.py').read())"
python odoo_invoices_export.py   # Avro schema parse + SQL smoke
```

To run for real you need `refined.odoo_invoices`, a dbt Cloud job
id in Airflow Variables, and event-API OAuth + schema id. This
folder is a sanitized reference, not a deploy.

## Sanitization notes

- GCP projects `hd-dwh-stream-*` → `dwh_project` / `dwh_project_dev`
- Dataset/table `dwh_refined.dana_odoo_invoices` →
  `refined.odoo_invoices`
- Column renames: `metro_id` → `wholesale_id`,
  `net_price_Eur` → `net_price_eur`
- Partner / brand names and ticket hosts removed
- Event API host / schema ids / OAuth Variable names generalized
- Hard-coded dbt job id → Airflow Variable `dbt_job_odoo_invoices`
- Real notification emails → `dataops@example.com`
- Owner / author names removed
- Package imports `horeca_digital.*` → local modules
- Token stagger / retry helpers preserved (critical for parallel
  batches)
- Prod vs sandbox ingest path-prefix difference externalized as
  `event_api_ingest_base` Variables

## Category

`odoo_integration/84-odoo-invoices-partner-export/`
