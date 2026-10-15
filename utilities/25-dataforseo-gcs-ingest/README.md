# Pattern 25: SEO listing GCS ingest (vendor NDJSON → BQ)

Landing-zone ingest for multi-country SEO / business-listing dumps.
Vendor (or ops) drops `.json` / `.json.gz` NDJSON into `uploads/`.
Composer stream-decompresses into `stg_to_load/`, preserves raw bytes
in `archive_raw/`, loads truncate into BigQuery staging, runs dbt, then
flattens stage objects into `archive_ingested/`.

Distinct from pattern 18 (menu URL extraction from already-refined
listings). This pattern is the *ingress* — magic-byte compression
detection, content-md5 metadata, dry-run CLI, and the four-prefix
bucket contract. Pattern 18 starts after listings are already in BQ.

Source (read-only):
- `dags/etl_dataforseo_ingestion.py`
- `dags/horeca_digital/dataforseo_gcs_ingest.py`

## Files

| File | Role |
|------|------|
| `gcs_ingest.py` | Scan / ingest / archive helpers + CLI |
| `dag_seo_listings_ingestion.py` | Composer: ingest → GCSToBQ → dbt → archive |
| `BUSINESS_CASE.md` | Why four prefixes + dry-run by default |
| `ARCHITECTURE.md` | Components + Mermaid diagram |
| `DATA_FLOW.md` | Object naming, metadata, failure modes |

## Quick start

```bash
python -c "import ast; ast.parse(open('gcs_ingest.py').read())"
python -c "import ast; ast.parse(open('dag_seo_listings_ingestion.py').read())"
python gcs_ingest.py scan /path/to/dump.json.gz
python gcs_ingest.py ingest --bucket seo-listings-ingest
python gcs_ingest.py archive --bucket seo-listings-ingest
```

Ingest/archive default to dry-run JSON previews. Pass `--apply` to
mutate GCS. Needs ADC (or Composer GCP conn) and the ingest bucket.
This folder is a sanitized reference, not a deploy package.

## Sanitization notes

- Bucket `dish-digital-dwh-dataforseo` → `seo-listings-ingest`
- GCP projects `hd-dwh-stream-*` → `dwh_project` / `dwh_project_dev`
- Schema buckets `hd-digital-dp-*-rawzone` → `rawzone` / `rawzone_dev`
- Dataset `dwh_trusted_staging` → `trusted_staging`
- Table `seo_establishments` → `seo_business_listings`
- Schema object `dataforseo_business_listing.json` →
  `seo_business_listing.json`
- Hardcoded dbt Cloud job id → Airflow Variable
  `seo_listings_dbt_job_id` (EmptyOperator when unset)
- Custom `DbtCloudRunJobOperator` import → try provider, else stub
- Owner / emails → `data-platform` / `dataops@example.com`
- Jira / internal ticket links removed
- Package imports `horeca_digital.*` → local `gcs_ingest`
- Sample establishment address anonymized

## Distinct from pattern 18

| | 18 | 25 (this) |
|---|----|-----------|
| Question | Find menu URLs from listing websites | Land vendor listing dumps into BQ |
| Input | Refined BQ listings | GCS `uploads/` NDJSON |
| Output | Extracted menu URL table | Staging listings + archived raw |
| Hard part | HTML discovery + Playwright fallback | Stream gunzip, metadata, four-prefix contract |

## Category

`utilities/25-dataforseo-gcs-ingest/`
