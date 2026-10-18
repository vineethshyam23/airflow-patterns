# Pattern 04: Mailchimp campaign analytics integration

Daily sync of six Mailchimp Marketing API report entities into BigQuery for
campaign performance, click tracking, audience engagement, and unsubscribe
analysis.

Source (read-only):
- `dags/etl_mailchimp.py`
- `dags/horeca_digital/mailchimp.py`

## Files

| File | Role |
|------|------|
| `mailchimp_client.py` | API client, pagination, JSONL extraction for 6 entities |
| `dag_mailchimp.py` | Composer DAG: fetch chain → GCS → BQ staging → trusted copy |
| `schemas/*.json` | BigQuery load schemas (one per entity) |
| `examples/*_sample.jsonl` | Synthetic JSONL output samples (no real PII) |
| `BUSINESS_CASE.md` | Why marketing needed warehouse-native campaign data |
| `ARCHITECTURE.md` | Components + Mermaid diagram |
| `DATA_FLOW.md` | Run order, 90-day window, retry behavior |

## Quick start

```bash
python -c "import ast; ast.parse(open('mailchimp_client.py').read())"
python -c "import ast; ast.parse(open('dag_mailchimp.py').read())"
```

To actually run anything you need a Mailchimp API key in an Airflow Variable
(`mailchimp_apikey`), server prefix (`mailchimp_server`, e.g. `us1`), a Composer
bucket for the JSONL landing zone, raw GCS + staging/trusted tables, and BQ
schemas uploaded to the Composer bucket. This folder is a sanitized reference,
not a deploy.

## Sanitization notes

- GCP projects `hd-dwh-stream-*` → `dwh_project` / `dwh_project_dev`
- Raw buckets → `dp_rawzone` / `dp_dev_rawzone`
- Staging dataset `dwh_trusted_staging` → `trusted_staging`
- Package import `horeca_digital.mailchimp` → local `mailchimp_client`
- Dutch merge field keys (`DEBNR`, `BRANCHE`, etc.) → generic CRM fields
- Real notification emails → `dataops@example.com`
- Commented `__main__` block with hardcoded API key removed entirely
- Example JSONL uses synthetic emails and generic company names only

Never commit real Mailchimp API keys or production audience data.

## Category

`marketing_integration/04-mailchimp-campaign-analytics/`
