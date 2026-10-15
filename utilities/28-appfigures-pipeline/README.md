# Pattern 28: AppFigures weekly mobile analytics ingest

Weekly Composer pipeline that pulls sales and ratings CSVs from the
AppFigures API, lands them in a rawzone path keyed by week end-date,
loads truncate into BigQuery staging, appends into trusted, then
triggers a dbt Cloud job behind a fan-in barrier.

Distinct from pattern 25 (SEO listing file-drop ingress) and pattern
19 (Freshdesk REST). This is a scheduled vendor analytics API with
four parallel report grains and a staging → trusted append contract.

Source (read-only):
- `dags/etl_appfigures_pipeline.py`
- `dags/horeca_digital/get_appfigures_data.py`

## Files

| File | Role |
|------|------|
| `fetch_appfigures_data.py` | HTTP fetch + Composer CSV write |
| `dag_appfigures_pipeline.py` | Four parallel chains + dbt barrier |
| `BUSINESS_CASE.md` | Why weekly + staging/trusted split |
| `ARCHITECTURE.md` | Components + Mermaid diagram |
| `DATA_FLOW.md` | Report grains, dates, failure modes |

## Quick start

```bash
python -c "import ast; ast.parse(open('fetch_appfigures_data.py').read())"
python -c "import ast; ast.parse(open('dag_appfigures_pipeline.py').read())"
```

Needs Airflow Variables `env`, `composer_bucket`,
`appfigures_auth_token`, and optionally `appfigures_dbt_job_id`.
This folder is a sanitized reference, not a deploy package.

## Sanitization notes

- GCP projects `hd-dwh-stream-*` → `dwh_project` / `dwh_project_dev`
- Buckets `hd-digital-dp-*-rawzone` → `rawzone` / `rawzone_dev`
- Datasets `dwh_trusted_staging` / `dwh_trusted` →
  `trusted_staging` / `trusted`
- Hardcoded Bearer PAT removed → Variable `appfigures_auth_token`
- Hardcoded dbt job id → Variable `appfigures_dbt_job_id`
  (EmptyOperator when unset)
- Owner / emails → `data-platform` / `dataops@example.com`
- Package import `horeca_digital.get_appfigures_data` → local module
- Product brand names generalized to "mobile apps"
- Fetch helper now fails on non-200 (production wrote bodies blindly)

## Category

`utilities/28-appfigures-pipeline/`
