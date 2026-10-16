# Pattern 77: Hydra Adobe traffic CSV publish

Daily Composer job that reads five refined Hydra / Adobe site-traffic
metric tables from BigQuery, writes dated CSVs to Composer staging,
then publishes them to a consumer bucket with delete-then-copy
overwrite and 60-day retention.

This is the *publish* path for already-shaped traffic marts. It is
not the Adobe Data Feed land (patterns 49 / 51) — those unpack hit
TSV into staging/trusted. Here we assume refined tables exist and
hand flat files to teams that will not query BigQuery.

Source (read-only):
- `dags/etl_adobe_traffic_data.py`

## Files

| File | Role |
|------|------|
| `traffic_csv_export.py` | Extract, device remap, overwrite, copy, retention, staging cleanup |
| `dag_hydra_adobe_traffic_csv.py` | Daily graph: retain → extract → overwrite → copy → summarize → cleanup |
| `BUSINESS_CASE.md` | Why CSV publish from refined instead of shared BQ or re-land |
| `ARCHITECTURE.md` | Components + Mermaid diagram |
| `DATA_FLOW.md` | Window, naming, failure modes |

## Quick start

```bash
python -c "import ast; ast.parse(open('traffic_csv_export.py').read())"
python -c "import ast; ast.parse(open('dag_hydra_adobe_traffic_csv.py').read())"
```

Needs `google_cloud_default` plus Variables `dwh_project_id`,
`refined_dataset`, `composer_bucket`, optional
`site_traffic_staging_folder` / `site_traffic_publish_bucket`, and
populated `refined.site_traffic_*` tables. This folder is a sanitized
reference, not a deploy package.

## Sanitization notes

- GCP project `hd-dwh-stream-1` → Variable `dwh_project_id` (`dwh_project`)
- Dataset `dwh_refined` → `refined`
- Composer bucket `dish-digital-dwh-composer-2` → `composer-bucket`
- Staging folder `data/adobe-traffic` → `data/site-traffic`
- Publish bucket `hd-digital-dp-refinedzone-hydra-traffic` → `refinedzone-site-traffic`
- Tables `hydra_traffic_*` → `site_traffic_*`
- DAG id `etl_adobe_traffic_data` → `etl_site_traffic_csv_publish`
- Owner / emails → `data-platform` / `dataops@example.com`
- Log emoji stripped; verbose brochure docstring trimmed to this README
- Config moved to Airflow Variables with defaults (source used hard-coded IDs)

## Distinct from nearby patterns

| | 49 / 51 (Data Feed land) | 39 (Hydra Cloud SQL dump) | 77 (this) |
|---|---|---|---|
| Direction | Vendor → warehouse | Cloud SQL → GCS/BQ | Warehouse → consumer files |
| Grain | Hit / lookup | Full table dump | Daily metric CSVs |
| Hard part | tar/tsv unpack + refined enrich | Weekly export operator | Overwrite + retention + staging gate |

## Category

`utilities/77-hydra-adobe-traffic-csv-export/`
