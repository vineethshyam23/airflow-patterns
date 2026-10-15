# Pattern 31: Maileon email marketing import

Daily Composer DAG that pulls eight Maileon engagement reports
(opens, unique opens, clicks, unique clicks, bounces, blocks,
unsubscriptions, recipients) over REST, lands JSONL under the
Composer data volume, branches past empty extracts, copies into
rawzone, truncate-loads BigQuery staging, then runs dbt plus
per-mailing name/tag enrichment.

Distinct from pattern 19 (Freshdesk tickets) and pattern 29
(Vonage contact-center stats): this is marketing-ESP XML reports
with an empty-file BranchPythonOperator and a second-wave metadata
fan-out that hits the API once per mailing_id.

Source (read-only):
- `dags/etl_maileon_import.py`
- `dags/horeca_digital/maileon.py`
- `dags/horeca_digital/get_maileon_names.py`

## Files

| File | Role |
|------|------|
| `maileon_api.py` | REST client, XML→JSONL, schema mapping |
| `get_maileon_metadata.py` | Per-mailing name + tags enrichment |
| `dag_maileon_pipeline.py` | 8-branch DAG + dbt + metadata chain |
| `BUSINESS_CASE.md` | Why marketing engagement lands this way |
| `ARCHITECTURE.md` | Components + Mermaid diagram |
| `DATA_FLOW.md` | Steps, empty-file branch, failure modes |

## Quick start

```bash
python -c "import ast; ast.parse(open('maileon_api.py').read())"
python -c "import ast; ast.parse(open('get_maileon_metadata.py').read())"
python -c "import ast; ast.parse(open('dag_maileon_pipeline.py').read())"
```

Needs Airflow Variables `maileon_apikey`, `composer_bucket`,
optionally `maileon_gcp_project`, `maileon_rawzone_bucket`, and
`maileon_dbt_{transform,names,api}_job_id`, plus schema JSON under
`schema_json/maileon_{report}.json`. This folder is a sanitized
reference, not a deploy package.

## Sanitization notes

- GCP projects `hd-dwh-stream-1` / `-dev` → Variables
  `maileon_gcp_project` (defaults `dwh_project` / `dwh_project_dev`)
- Buckets `hd-digital-dp-rawzone` / Composer bucket →
  `maileon_rawzone_bucket` / `composer_bucket`
- Datasets `dwh_trusted` / `dwh_trusted_staging` →
  `trusted` / `trusted_staging`
- Hardcoded dbt job IDs → Variables
- Owner / emails → `data-platform` / `dataops@example.com`
- Package imports `horeca_digital.*` → local modules
- Removed `main()` with embedded API key sample
- Replaced `eval(f"{report}_loc")` with a dict of local paths
- Collapsed duplicated `chain(...)` for names/tags into one linear
  chain (production loop created redundant edges)
- Prefer normalized `records` from XML parse when writing JSONL
  (production walked container keys after a flatten that already
  returned `records` — quiet empty files risk)
- Brand references in comments generalized to "platform"

## Category

`utilities/31-maileon-email-import/`
