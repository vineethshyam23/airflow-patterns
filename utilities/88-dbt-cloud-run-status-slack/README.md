# Pattern 88: dbt Cloud run status Slack digest

Daily Composer DAG that scans Airflow Variables for keys containing
`dbt_runids`, asks dbt Cloud for each run's outcome, keeps only
failures, and posts one Slack webhook message per failed job.

Distinct from pattern 80 (Slack on materialization failure aggregate)
and pattern 83 (BigQuery consumption cost digest): this is a scheduled
poll of *dbt Cloud* run status driven by Variable-stored run ids that
trigger DAGs write after they submit jobs.

Source (read-only):
- `dags/dbt_jobs_status.py`

## Files

| File | Role |
|------|------|
| `dag_dbt_cloud_run_status_slack.py` | Variable scan + dbt Cloud poll + Slack |
| `BUSINESS_CASE.md` | Why Variable-driven dbt failure digest |
| `ARCHITECTURE.md` | Components + Mermaid diagram |
| `DATA_FLOW.md` | Schedule, path, failure modes |

## Quick start

```bash
python -c "import ast; ast.parse(open('dag_dbt_cloud_run_status_slack.py').read())"
```

To run for real you need Airflow Variables with `dbt_runids` in the
key (JSON lists of run ids), a `dbt_cloud_conn` connection, a
`slack_webhook_conn`, and dbt Cloud account access. This folder is a
sanitized reference, not a deploy package.

## Sanitization notes

- Owner / emails → `data-platform` / `dataops@example.com`
- Slack channel `#hd-dwh-dbt` → `#dwh-dbt-ops`
- Connections `dbt_conn` / `slack_conn` → `dbt_cloud_conn` /
  `slack_webhook_conn`
- DEV/PROD GCP project labels → `dwh-stream-dev` / `dwh-stream-prod`
- `DbtCloudHook` prefers `hooks.dbt` with fallback to legacy
  `operators.dbt` re-export
- Slack operator kwarg `http_conn_id` → `slack_webhook_conn_id`
  (current provider)
- Dropped unused `EmptyOperator` import; kept bare job-name fallback
- Guarded null `finished_at` before `strptime` (prod would raise on
  in-flight runs that somehow reach Failed without a timestamp)
- Username typo "Integtation" → "Integration"
- Account id `3` kept as module constant `DBT_ACCOUNT_ID` — tenant-
  specific, not a secret

## Category

`utilities/88-dbt-cloud-run-status-slack/`

## Distinct from nearby patterns

| | 80 (MCC materialize) | 83 (BQ consumption) | 88 (this) |
|---|---|---|---|
| Signal source | Parallel BQ task failures | `monitoring.bq_usage` | dbt Cloud API via run ids |
| Discovery | Task instance states | Fixed SQL | Airflow Variables `*dbt_runids*` |
| Slack shape | One aggregate on failure | One daily cost digest | One message per failed dbt run |
| Owns job submit? | No (materialize only) | No | No (poll only) |
