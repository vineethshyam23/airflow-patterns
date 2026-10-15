# Pattern 38: POS vendor GA4 rolling event ingest

Daily Composer DAG that reloads a 7-day window of native GA4→BigQuery
export shards into a staging table (DELETE+INSERT per day), kicks a
BigQuery Data Transfer config, and runs one dbt Cloud job. Built for
late-arriving GA4 corrections without a full history reload.

Distinct from pattern 35 (same POS vendor, HMAC CSV store-details
master). This pattern is product web/app event analytics from GA4
export tables.

Source (read-only):
- `dags/etl_booq_google_analytics.py`

## Files

| File | Role |
|------|------|
| `dag_booq_ga4.py` | Composer DAG: 7 parallel day loads → transfer → dbt → runids |
| `ga4_transfer.py` | Manual Data Transfer kick + fixed wait; dbt run-id helper |
| `BUSINESS_CASE.md` | Why a rolling window beats full reload |
| `ARCHITECTURE.md` | Components + Mermaid diagram |
| `DATA_FLOW.md` | Run order, date coupling, failure modes |

## Quick start

```bash
python -c "import ast; ast.parse(open('ga4_transfer.py').read())"
python -c "import ast; ast.parse(open('dag_booq_ga4.py').read())"
```

To run for real you need GCP BigQuery + Data Transfer IAM, Variables
`booq_ga4_property_id`, `booq_ga4_transfer_config`,
`booq_ga4_dbt_job_id` (and optional wait seconds), DEV/PROD GCP
connections, and dbt Cloud. This folder is a sanitized reference, not
a deploy.

## Sanitization notes

- GCP project `hd-dwh-stream-1` → `dwh_project` / `dwh_project_dev`
- GA4 property id `288783069` → Variable `booq_ga4_property_id`
  (placeholder default `000000000`)
- Staging table `booq_ga_events` → `pos_vendor_ga_events`
- Data Transfer project number + config UUID → Variable
  `booq_ga4_transfer_config`
- dbt job numeric id → Variable `booq_ga4_dbt_job_id`
- Real notification emails → `dataops@example.com`
- Owner / author names removed from DAG body
- Extracted transfer + run-id helpers into `ga4_transfer.py`
- Optional dbt provider stub; `max_active_runs=1` on the DAG
- Kept production `dagrun_timeout=20m` and fixed transfer sleep;
  called out as debt in docs

## Category

`utilities/38-booq-ga4-rolling-ingest/`
