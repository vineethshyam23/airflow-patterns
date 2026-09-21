# Pattern 51: Adobe Analytics app Data Feed land

Hourly Composer job that unpacks the *mobile-app* Adobe Analytics Data
Feed from a GCS landing zone, loads hit TSV + a lean lookup set into
BigQuery staging, fans lookups into trusted dimensions, then appends a
mobile-centric refined hit table.

Sibling of pattern 49 (web-suite hourly). Same extract shape; different
report-suite stem, lookup cardinality, refined projection, and
idempotency posture.

Source (read-only):
- `dags/etl_aa_adobe_rawfeed_app.py`

## Files

| File | Role |
|------|------|
| `rawfeed_extract.py` | List / unpack tar.gz + gunzip tsv.gz + move to processed (app prefix) |
| `dag_adobe_analytics_app_rawfeed.py` | Hourly graph: extract → stage → trusted → refined |
| `refined_app_hit_query.sql` | App eVars, device/OS/carrier, visit windows |
| `BUSINESS_CASE.md` | Why a second lander instead of parameterizing #49 |
| `ARCHITECTURE.md` | Components + Mermaid diagram |
| `DATA_FLOW.md` | Object naming, lean fan-out, failure modes |

## Quick start

```bash
python -c "import ast; ast.parse(open('rawfeed_extract.py').read())"
python -c "import ast; ast.parse(open('dag_adobe_analytics_app_rawfeed.py').read())"
python rawfeed_extract.py   # dry list of landing candidates (needs GCS ADC)
```

Needs Composer Variables `composer_bucket`, optional `aa_landing_bucket` /
`dwh_project_id` / `rawzone_bucket`, plus `bigquery_default`. This folder
is a sanitized reference, not a deploy package.

## Sanitization notes

- Landing bucket `hd-digital-dp-landingzone` → `landingzone`
- Schema bucket `hd-digital-dp-rawzone` → `rawzone`
- GCP project `hd-dwh-stream-1` → `dwh_project` (Variable `dwh_project_id`)
- Datasets `dwh_trusted*` / `dwh_refined` → `trusted` / `trusted_staging` / `refined`
- Report suite stem `horecadigitaldishapp` → `app_report_suite`
- Refined table `adobe_datafeed_app` → `analytics_datafeed_app`
- Owner / emails → `data-platform` / `dataops@example.com`
- Job name `etl_adobe.py` → `etl_aa_app_rawfeed`
- Local package import for extract helpers (was inline in the source DAG)
- Proprietary eVar semantics kept as generic product / user / device labels

## Distinct from nearby patterns

| | 28 (AppFigures) | 49 (AA web hourly) | 51 (this) |
|---|---|---|---|
| Vendor | App store analytics API | Adobe Data Feed (web suite) | Adobe Data Feed (app suite) |
| Cadence | Weekly | Hourly | Hourly |
| Lookups | n/a | 13 web dims | 4 mobile dims |
| Hard part | Multi-report API + dbt | Dual extract + full fan-out + dedupe | Same lander shape, app projection, no hit_id guard |

## Category

`utilities/51-adobe-analytics-app-rawfeed/`
