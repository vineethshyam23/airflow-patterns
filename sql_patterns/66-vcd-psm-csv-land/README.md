# Pattern 66: VCD PSM uplift CSV land

Composer DAG that lands PSM (propensity / uplift) CSV objects from GCS
into `trusted_staging.vcd_psm_*` on the **3rd and 8th** of each month,
then triggers the VCD PSM dbt Cloud job.

Distinct from pattern 62 (BQ CREATE OR REPLACE staging + CALL
`get_psm_uplift_values_v2`). Pattern 62 *produces* the CSV objects via
the stored procedure; this pattern *consumes* them with a
file × country × env fan-out of `GCSToBigQueryOperator` loads.

Source (read-only):
- `dags/etl_value_creation_zone_3rd_and_8th_of_month_psm.py`
- `dags/horeca_digital/sql/vcdb_psm.sql` (optional MERGE reference)

## Files

| File | Role |
|------|------|
| `dag_vcd_psm_csv_land.py` | Schedule, fan-out, barrier, dbt, status |
| `vcdb_psm_merge.sql` | Optional country+month MERGE (commented out in prod DAG) |
| `BUSINESS_CASE.md` | Why a sibling DAG after the SP |
| `ARCHITECTURE.md` | Components + Mermaid diagram |
| `DATA_FLOW.md` | Paths A–D, schema split, failure modes |

## Quick start

```bash
python -c "import ast; ast.parse(open('dag_vcd_psm_csv_land.py').read())"
```

To run for real you need Composer with GCS + BigQuery + dbt Cloud
connections, the PSM CSV objects under
`PSM/{env}/{iso}/{yyyymm}/`, schema JSON under `PSM/schema/`, and
Airflow Variables `vcd_psm_gcs_bucket`, `vcd_psm_dbt_job_id`,
`vcd_psm_month` (and optionally `vcd_psm_gcp_conn` /
`vcd_psm_dbt_account_id`). This folder is a sanitized reference, not a
deploy package.

## Sanitization notes

- GCP project `vcdb-bq-5064` / discovery bucket `hd-digital-dp-discovery`
  → Variable `vcd_psm_gcs_bucket` (default `dwh_discovery_bucket`);
  destination kept as dataset `trusted_staging` (was
  `dwh_trusted_staging`)
- dbt job id / account id → Variables `vcd_psm_dbt_job_id` /
  `vcd_psm_dbt_account_id` (default `0`)
- Emails / Slack channel / webhook → `dataops@example.com` + log stub
- Author names removed; `max_active_runs=1` added
- Parse-time `datetime.now().strftime("%Y%m")` → Variable
  `vcd_psm_month` (fallback still UTC YYYYMM) so re-runs are operable
- `source_objects` normalized to a list (operator expects list-like)
- Inline MERGE to trusted_source left as `vcdb_psm_merge.sql` reference;
  production had that step commented out in favour of dbt
- DAG id renamed to `etl_value_creation_zone_psm_csv_land` for clarity

## Distinct from patterns 62 / 25 / 35

| | 62 | 25 / 35 | 66 (this) |
|---|----|---------|-----------|
| Role | BQ staging + CALL SP | Vendor ingest (SEO / POS HMAC) | PSM CSV land after SP |
| Source | DWH tables | Vendor NDJSON / HMAC CSV | GCS `PSM/{env}/{iso}/` |
| Fan-out | country × table SQL | usually single path | file × country × env |
| Barrier | EmptyOperator before SP | dbt / archive | EmptyOperator before dbt |
| Schema | SQL builders | vendor schema | schema JSON vs autodetect |

## Category

`sql_patterns/66-vcd-psm-csv-land/`
