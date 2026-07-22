# Pattern 08: Odoo WSL invoices dual export

Daily pipeline that refreshes a trusted Odoo wholesale (WSL) invoice
intermediate via dbt Cloud, APPEND-copies the same rowset into an ML
recommender history table, and pushes the full snapshot as Avro events
to an external ingest API for a pilot market.

Source (read-only):
- `dags/etl_dana_odoo_wsl_invoices_export.py`
- `dags/horeca_digital/dana_odoo_wsl_invoices.py`

## Files

| File | Role |
|------|------|
| `wsl_invoices_query.py` | Shared SELECT for send + recommender copy |
| `wsl_invoices_export.py` | OAuth client, Avro encode, chunked bulk POST |
| `dag_wsl_invoices_export.py` | Composer DAG: dbt → APPEND → ingest |
| `BUSINESS_CASE.md` | Why one DAG owns both sinks |
| `ARCHITECTURE.md` | Components + Mermaid diagram |
| `DATA_FLOW.md` | Run order, idempotency, failure modes |

## Quick start

```bash
python -c "import ast; ast.parse(open('wsl_invoices_query.py').read())"
python -c "import ast; ast.parse(open('wsl_invoices_export.py').read())"
python -c "import ast; ast.parse(open('dag_wsl_invoices_export.py').read())"
python wsl_invoices_query.py   # prints the shared SELECT
```

To run for real you need the trusted intermediate, a dbt Cloud job id in
Airflow Variables, event-API OAuth + schema id, and the recommender
dataset. This folder is a sanitized reference, not a deploy.

## Sanitization notes

- GCP projects `hd-dwh-stream-*` → `dwh_project` / `dwh_project_dev`
- Dataset `dwh_trusted` → `trusted`; `dwh_recommender_vertex` →
  `ml_recommender`
- Table `int_dana_odoo_wsl_invoices` → `int_odoo_wsl_invoices`
- Event API host / schema ids / OAuth Variable names generalized
- Hard-coded dbt job id → Airflow Variable `dbt_job_odoo_wsl_invoices`
- Real notification emails → `dataops@example.com`
- Owner / author names removed
- Package import `horeca_digital.dana_odoo_wsl_invoices` → local modules
- `DummyOperator` → `EmptyOperator` (with fallback)
- `catchup=True` → `False` (documented; APPEND + catchup duplicates)
- Avro schema parse moved outside the per-row loop
- Fixed `theo_total_rec_revenue` mapping (source read onetime column)
- Dropped unused GCS bucket variables from the DAG

## Category

`odoo_integration/08-wsl-invoices-export/`
