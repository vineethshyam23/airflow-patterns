# Pattern 68: Sales data mart two-phase copy

Composer DAG that refreshes a Sales-facing BigQuery mart
(`refined_sales`) in two phases: materialize analytical views via
`SELECT *` insert jobs, pause, then native BigQuery-to-BigQuery
WRITE_TRUNCATE of physical tables. Permission boundary for Sales / BI
without IAM on the full trusted layer.

Distinct from pattern 59 (builds CRM-shaped aggregates with SQL) and
from pattern 67 (builds the refined analytical actuals). This pattern
owns the *identity copy into a team dataset* contract and the
view-vs-table operator split.

Source (read-only):
- `dags/etl_dwh_sales_export.py`

## Files

| File | Role |
|------|------|
| `dag_sales_datamart_two_phase_copy.py` | Catalogs, phase chain, ENV project |
| `BUSINESS_CASE.md` | Why a mart + two operators |
| `ARCHITECTURE.md` | Components + Mermaid diagram |
| `DATA_FLOW.md` | Paths A–B, freeze note, failure modes |

## Quick start

```bash
python -c "import ast; ast.parse(open('dag_sales_datamart_two_phase_copy.py').read())"
```

To run for real you need Composer with BigQuery, Variable `env`
(DEV/PROD), project Variables `dwh_gcp_project` /
`dwh_gcp_project_dev`, GCP connections, source views/tables, and a
`refined_sales` dataset. This folder is a sanitized reference, not a
deploy package.

## Sanitization notes

- GCP projects `hd-dwh-stream-1` / `hd-dwh-stream-1-dev` → Variables
  `dwh_gcp_project` / `dwh_gcp_project_dev` (defaults `dwh_project` /
  `dwh_project_dev`)
- Destination dataset `dwh_refined_sales` → `refined_sales`
- Source datasets `dwh_trusted_views` / `dwh_refined` /
  `dwh_trusted_odoo` → `trusted_views` / `refined` / `trusted_odoo`
- View prefixes `aac_*` / `ahyd_*` / `apc_*` / `aodoo_*` →
  `a_subscription_*` / `a_hyd_*` / `a_catalog_*` / `a_odoo_*`
- Table `analytical_sfdc_establishment_actual` →
  `analytical_crm_establishment_actual`
- Emails → `dataops@example.com`; owner → `data-platform`
- Removed unused `bucket_name` / `bigquery_conn_id` /
  deprecated `provide_context`
- `DummyOperator` → `EmptyOperator`
- Added `max_active_runs=1` and DAG tags
- **Fixed** production DEV bug: view `destinationTable.projectId` was
  hard-coded to prod; sanitized code uses ENV `PROJECT_ID` for both
  phases
- Live catalog only (post-CRM freeze). Commented SFDC object lists from
  production are documented, not re-shipped as dead code
- Soft trigger rule `none_failed_min_one_success` preserved and called
  out as partial-freshness risk

## Distinct from patterns 59 / 67 / 05

| | 05 | 59 | 67 | 68 (this) |
|---|----|----|----|-----------|
| Role | Asset hist → Avro | Build CRM snapshots | Build refined SCD spine | Copy into Sales mart |
| Transform | Hash-delta export | SQL aggregates | Hash SCD2 | Identity |
| Destination | Event bus | `refined_salesforce` | `refined` | `refined_sales` |

## Category

`sql_patterns/68-sales-datamart-two-phase-copy/`
