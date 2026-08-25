# Pattern 25: SEO business-listing GCS ingest

Vendor SEO dumps land as gzipped (or plain) NDJSON under `uploads/`.
This pattern promotes them into a loadable staging prefix, truncates
into BigQuery staging, triggers dbt, then archives — without losing
vendor bytes.

Distinct from pattern 18 (menu URL extraction from refined listings).
This is the dump → warehouse landing path; pattern 18 is the crawl
that comes later.

Source (read-only):
- `dags/etl_dataforseo_ingestion.py`
- `dags/horeca_digital/dataforseo_gcs_ingest.py`
- `dags/schema_json/dataforseo_business_listing.json`

## Files

| File | Role |
|------|------|
| `seo_gcs_ingest.py` | Stream scan / promote / archive helpers + CLI |
| `dag_seo_gcs_ingest.py` | Composer: promote → BQ truncate → dbt → archive |
| `schema_seo_business_listing.json` | BQ schema for staging load |
| `BUSINESS_CASE.md` | Why linear chain + post-dbt archive |
| `ARCHITECTURE.md` | Components + Mermaid diagram |
| `DATA_FLOW.md` | Prefix layout, failure modes, CLI dry-run |

## Quick start

```bash
python -c "import ast; ast.parse(open('seo_gcs_ingest.py').read())"
python -c "import ast; ast.parse(open('dag_seo_gcs_ingest.py').read())"
python seo_gcs_ingest.py scan /path/to/dump.json.gz
```

Needs a GCS ingest bucket with the four prefixes, schema JSON in the
rawzone bucket, and (prod) Airflow Variables for the dbt job id.
This folder is a sanitized reference, not a deploy package.

## Sanitization notes

- GCP projects `hd-dwh-stream-*` → `dwh_project` / `dwh_project_dev`
- Buckets `hd-digital-dp-*-rawzone` → `data-platform-*-rawzone`
- Ingest bucket `dish-digital-dwh-dataforseo` → `dwh-seo-business-listing`
- Dataset `dwh_trusted_staging` → `staging`
- Hard-coded dbt job id → Variables `dbt_seo_listings_job_id` /
  `dbt_seo_listings_job_id_dev` (empty → EmptyOperator)
- Custom `horeca_digital.operators.dbt.DbtCloudRunJobOperator` →
  `airflow.providers.dbt.cloud.operators.dbt.DbtCloudRunJobOperator`
- Package import `horeca_digital.dataforseo_gcs_ingest` → local
  `seo_gcs_ingest`
- Owner / author names, notification emails, Jira ticket URLs removed
- Vendor product name generalized to "SEO business listing" in docs /
  comments; engineering layout preserved

## Distinct from pattern 18

| | Pattern 18 | Pattern 25 (this) |
|---|------------|-------------------|
| Question | Where is the menu URL? | How do vendor dumps land in the warehouse? |
| Input | Refined SEO listings in BQ | Raw NDJSON dumps in GCS `uploads/` |
| Output | Extracted menu URL table | Staging table + dbt refine + archived dumps |
| Cadence | Per-country Composer batches | Manual / on-demand |

## Category

`utilities/25-seo-gcs-ingest/`
