# Pattern 43: Midday POS customer-master refresh

Composer DAG that refreshes POS vendor debtor + location dumps at
13:00 Europe/Amsterdam, reloads BigQuery staging, re-runs the shared
customer / matching / POS dbt chain, and materializes the refined
customer-base view as a table.

Distinct from pattern 35 (HMAC store-details API) and pattern 38
(GA4 rolling events): same vendor drop zone as overnight POS, but a
selective midday path for customer master only, with a DST-safe
local timetable.

Source (read-only):
- `dags/etl_dish_pos_afternoon.py`

## Files

| File | Role |
|------|------|
| `customer_master_load.py` | Latest same-day blob select + staging TRUNCATE load |
| `dag_pos_afternoon_refresh.py` | Timetable, dual load fan-in, dbt chain, materialize, Slack |
| `BUSINESS_CASE.md` | Why afternoon is a second DAG, not a second full POS run |
| `ARCHITECTURE.md` | Components + Mermaid diagram |
| `DATA_FLOW.md` | Steps, soft-skip, failure modes |

## Quick start

```bash
python -c "import ast; ast.parse(open('customer_master_load.py').read())"
python -c "import ast; ast.parse(open('dag_pos_afternoon_refresh.py').read())"
```

To run for real you need Airflow Variables for project / drop bucket /
datasets, optional dbt Cloud job ids, and Slack webhook connection.
This folder is a sanitized reference, not a deploy package.

## Sanitization notes

- GCP project `hd-dwh-stream-1` → Variable `dwh_project` (default
  `dwh_project`)
- Bucket `hd-dwh-toaster-dev-pos-data` → Variable
  `pos_vendor_drop_bucket` (default `pos-vendor-drop`)
- Datasets `dwh_trusted_staging` / `dwh_refined` →
  `trusted_staging` / `refined`
- Tables `booq_debtor` / `booq_debloc` → `vendor_debtor` /
  `vendor_location`
- Object prefixes `Eijsink-Debtor` / `Eijsink-DebLoc` →
  `Vendor-Debtor` / `Vendor-DebLoc`
- Hardcoded dbt job ids → Variables `pos_afternoon_dbt_*_job_id`
- Emails / Slack channel / owners → `dataops@example.com` /
  `#pos-data-ops` / `data-platform`
- Package import `horeca_digital.operators.dbt` → optional
  `airflow.providers.dbt.cloud` with EmptyOperator stub
- Removed unused `get_rowhash` / `get_keyhash` (SCD stays overnight)
- Removed emoji / personal emails from Slack failure text
- Dropped large commented status-notification block from source
- Kept parse-time date token behaviour; documented backfill caveat
- `max_active_runs=1`; soft-skip when no same-day blob

## Category

`utilities/43-pos-afternoon-customer-refresh/`
