# Pattern 34: Mailchimp email analytics ingest

Daily Composer DAG that pulls six Mailchimp Marketing API grains
(campaign list, campaign reports, click URLs, unsubscribes, email
activity, recipients), lands JSONL on the Composer data volume, then
fans out Composer → rawzone → BigQuery staging (APPEND) → trusted
(WRITE_TRUNCATE).

Distinct from pattern 31 (Maileon XML reports + empty-file branch +
dbt): this uses the official Mailchimp SDK, a 90-day campaign-id
lookup to drive per-campaign report fan-out, and a ten-attempt retry
loop per campaign instead of parallel report-type branches.

Source (read-only):
- `dags/etl_mailchimp.py`
- `dags/horeca_digital/mailchimp.py`

## Files

| File | Role |
|------|------|
| `mailchimp_api.py` | SDK client, pagination, JSONL flatteners |
| `dag_mailchimp_pipeline.py` | Serial extract + parallel land/load DAG |
| `BUSINESS_CASE.md` | Why campaign-scoped ESP data lands this way |
| `ARCHITECTURE.md` | Components + Mermaid diagram |
| `DATA_FLOW.md` | Steps, campaign-ID dependency, failure modes |

## Quick start

```bash
python -c "import ast; ast.parse(open('mailchimp_api.py').read())"
python -c "import ast; ast.parse(open('dag_mailchimp_pipeline.py').read())"
```

Needs Airflow Variables `mailchimp_apikey`, `mailchimp_server_prefix`,
`composer_bucket`, optionally `mailchimp_gcp_project` /
`mailchimp_rawzone_bucket`, plus schema JSON under
`schema_json/{entity}.json` in the rawzone bucket. This folder is a
sanitized reference, not a deploy package.

## Sanitization notes

- GCP projects `hd-dwh-stream-1` / `-dev` → Variable
  `mailchimp_gcp_project` (defaults `dwh_project` / `dwh_project_dev`)
- Buckets `hd-digital-dp-rawzone` / Composer bucket →
  `mailchimp_rawzone_bucket` / `composer_bucket`
- Datasets `dwh_trusted` / `dwh_trusted_staging` →
  `trusted` / `trusted_staging`
- Hardcoded server `us3` → Variable `mailchimp_server_prefix`
- Owner / emails → `data-platform` / `dataops@example.com`
- Package imports `horeca_digital.mailchimp` → local `mailchimp_api`
- Removed commented `main` block with embedded API key sample
- Dutch CRM merge-tag names → generic keys
  (`ACCOUNT_NO`, `SEGMENT`, `COMPANY`, …); remap to your list tags
- Sample campaign ID defaults removed; campaign_id is required
- Task id `copy_table_to_staging_mailchimp_*` renamed to
  `copy_table_to_trusted_mailchimp_*` (destination was always trusted)
- Brand / regional ESP labels in DAG header generalized

## Category

`utilities/34-mailchimp-email-analytics/`
