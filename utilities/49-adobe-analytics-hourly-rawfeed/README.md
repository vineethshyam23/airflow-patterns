# Pattern 49: Adobe Analytics hourly Data Feed land

Hourly Composer job that unpacks Adobe Analytics Data Feed drops from a
GCS landing zone, loads hit TSV + lookup key/value dumps into BigQuery
staging, fans lookups into trusted dimensions, then appends an enriched
refined hit table with a one-day `hit_id` dedupe window.

This is the *web suite* hourly path. Sibling DAGs (app rawfeed, dbt-migrated
app job) share the same extract shape but different prefixes and
downstream SQL ownership — not shipped here.

Source (read-only):
- `dags/etl_aa_adobe_rawfeed_hourly.py`

## Files

| File | Role |
|------|------|
| `rawfeed_extract.py` | List / unpack tar.gz + gunzip tsv.gz + move to processed |
| `dag_adobe_analytics_hourly_rawfeed.py` | Hourly graph: extract → stage → trusted → refined |
| `refined_hit_query.sql` | Lookup joins, visit/hit IDs, IP hash, append dedupe |
| `BUSINESS_CASE.md` | Why land in Composer instead of a pure BQ transfer |
| `ARCHITECTURE.md` | Components + Mermaid diagram |
| `DATA_FLOW.md` | Object naming, fan-out, failure modes |

## Quick start

```bash
python -c "import ast; ast.parse(open('rawfeed_extract.py').read())"
python -c "import ast; ast.parse(open('dag_adobe_analytics_hourly_rawfeed.py').read())"
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
- Report suite stem `horecadigitallive2018` → `web_report_suite`
- Refined table `adobe_datafeed` → `analytics_datafeed`
- Owner / emails → `data-platform` / `dataops@example.com`
- Proprietary product eVar/prop remaps trimmed to a representative set
- Job name `etl_adobe.py` → `etl_aa_hourly_rawfeed`
- Local package import for extract helpers (was inline in the source DAG)

## Distinct from nearby patterns

| | 25 (SEO GCS) | 38 (GA4 rolling) | 49 (this) |
|---|---|---|---|
| Vendor | SEO listing dumps | GA4 Data Transfer | Adobe Data Feed |
| Cadence | Manual / on-drop | Daily rolling 7d | Hourly |
| Hard part | Stream gunzip + four-prefix contract | DELETE+INSERT window | Dual extract + lookup fan-out + refined enrich |

## Category

`utilities/49-adobe-analytics-hourly-rawfeed/`
