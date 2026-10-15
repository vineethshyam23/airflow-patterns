# Pattern 33: Jira Service Desk ingest

Twice-daily Composer pipeline that pulls Jira Cloud issues for one or
more support projects, lands JSONL (ADF-flattened description/comments
+ changelog), appends into BigQuery JSON staging, and triggers dbt for
normalization / dedupe. Supports an optional full-history mode that
fans out monthly TaskGroup tasks from the project's real date span.

Distinct from pattern 19 (Freshdesk REST) and pattern 29 (Vonage
contact-center stats): this is issue-tracker history with ADF parsing,
changelog expand, and a dual incremental/full-load graph shaped like
the Odoo EDI rank-split pattern.

Source (read-only):
- `dags/etl_jira_HDSD.py`
- `dags/horeca_digital/jira_hdsd.py`

## Files

| File | Role |
|------|------|
| `jira_client.py` | Creds at runtime, ADF flatten, JQL pagination, date-range probe |
| `dag_jira_ingest.py` | Incremental vs monthly TaskGroup full load + GCS/BQ/dbt |
| `BUSINESS_CASE.md` | Why support tickets land in the warehouse this way |
| `ARCHITECTURE.md` | Components + Mermaid diagram |
| `DATA_FLOW.md` | Modes, failure table, grain |

## Quick start

```bash
python -c "import ast; ast.parse(open('jira_client.py').read())"
python -c "import ast; ast.parse(open('dag_jira_ingest.py').read())"
```

Needs Airflow Variables `jira_service_desk_creds` (JSON username +
api_token), `jira_project_keys`, `composer_bucket`, and optionally
`jira_gcp_project`, `jira_rawzone_bucket`, `jira_dbt_job_id`,
`jira_base_url`. This folder is a sanitized reference, not a deploy
package.

## Sanitization notes

- Atlassian site host → `example.atlassian.net` / Variable `jira_base_url`
- Project keys HDSD / POSAPP → SUP / POSAPP (configurable via Variable)
- GCP projects `hd-dwh-stream-*` → `dwh_project` / `dwh_project_dev`
- Buckets / datasets → `rawzone`, `trusted_staging`
- Owner emails → `dataops@example.com`
- Hardcoded dbt job id → Variable `jira_dbt_job_id`
- Package import `horeca_digital.jira_hdsd` → local `jira_client`
- Dropped emoji-heavy progress prints; kept checkpoint + rate-limit behaviour
- Normalized inconsistent helper names from the production module

## Category

`utilities/33-jira-service-desk-ingest/`
