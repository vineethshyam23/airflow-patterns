# Pattern 83: BigQuery consumption Slack digest

Daily Composer DAG that reads a pre-aggregated BigQuery usage table,
ranks yesterday's top human consumers by GiB scanned, annotates each
with a GB/query efficiency ratio, calls out the heaviest user on
record, and posts month-to-date totals to a Slack ops channel.

Distinct from pattern 80 (Slack on materialization failure
aggregate): this is a scheduled cost-awareness digest, not a failure
reporter. Upstream land of `monitoring.bq_usage` is out of scope.

Source (read-only):
- `dags/bq_consumption_statistics_slack.py`

## Files

| File | Role |
|------|------|
| `dag_bq_consumption_slack_report.py` | SQL + get_status + DAG |
| `BUSINESS_CASE.md` | Why a daily social cost signal |
| `ARCHITECTURE.md` | Components + Mermaid diagram |
| `DATA_FLOW.md` | Schedule, path, failure modes |

## Quick start

```bash
python -c "import ast; ast.parse(open('dag_bq_consumption_slack_report.py').read())"
```

To run for real you need read IAM on `monitoring.bq_usage`, an
Airflow connection `slack_webhook_conn`, and yesterday's partition
populated. This folder is a sanitized reference, not a deploy package.

## Sanitization notes

- Dataset `dwh_monitoring` → `monitoring`
- Slack channel `#hd-dwh` → `#dwh-ops`
- Connection `slack_cofg_conn` → `slack_webhook_conn`
- Owner / emails → `data-platform` / `dataops@example.com`
- Author names / ticket headers removed from the production docstring
- Deprecated `airflow.contrib` / `python_operator` imports → providers
  where available, with BigQueryHook fallback for older Composer
- Dropped unused `os` / `date` imports; kept unused connection
  password read to mirror production
- Documented the "30 day record" vs all-time SQL mismatch instead of
  silently "fixing" it away
- Slack emoji shortcodes in SQL/message kept — they are Slack markup,
  not documentation fluff

## Category

`utilities/83-bq-consumption-slack-report/`
