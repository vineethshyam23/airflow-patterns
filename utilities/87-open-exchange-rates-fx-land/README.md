# Pattern 87: Open Exchange Rates FX land

Daily Composer pipeline that pulls USD-base FX from Open Exchange
Rates (`/api/latest.json`), lands a nested JSONL snapshot, truncates
staging, and appends currency rows with EUR- and USD-relative factors
into trusted. Idempotent on API `timestamp`.

This is the *single FX API → nested JSON → SQL cross-rate append*
pattern. It is not multi-entity CSV hash-delta land (pattern 86) and
not AppFigures weekly CSV report fan-out (pattern 28).

Source (read-only):
- `dags/etl_oex.py`
- `dags/horeca_digital/exchangerates.py` (legacy alternate extract)

## Files

| File | Role |
|------|------|
| `oex_extract.py` | HTTP fetch, nested JSONL reshape, FX insert SQL |
| `dag_oex_fx_land.py` | Four-task sequential land chain |
| `schema_json/oex_data.json` | Nested staging schema for NDJSON load |
| `BUSINESS_CASE.md` | Why nested staging + EUR factors in SQL |
| `ARCHITECTURE.md` | Components + Mermaid diagram |
| `DATA_FLOW.md` | Paths, columns, failure modes |

## Quick start

```bash
python -c "import ast; ast.parse(open('oex_extract.py').read())"
python -c "import ast; ast.parse(open('dag_oex_fx_land.py').read())"
```

Needs Airflow connection `api_openexchangerates` (app_id in password),
Variables for project / composer bucket / rawzone / datasets, and
`schema_json/oex_data.json` on the load bucket. This folder is a
sanitized reference, not a deploy package.

## Sanitization notes

- GCP project `hd-dwh-stream-1` → Variable `dwh_project_id`
- Raw bucket `hd-digital-dp-rawzone` → Variable `rawzone_bucket`
- Datasets `dwh_trusted_staging` / `dwh_trusted` →
  `trusted_staging` / `trusted`
- Owner / emails → `data-platform` / `dataops@example.com`
- Contrib GCS operators → current `providers.google` transfers
- Inline `oex()` in DAG → `oex_extract.py`; SQL extracted to
  `build_fx_insert_sql`
- Parse-time `date.today()` object name → `{{ ds }}`
- Extract now raises on non-200 (production wrote nothing and stayed green)
- Legacy `exchangerates.py` (env-module + direct rawzone upload) not
  shipped as a second path — same reshape contract

## Distinct from nearby patterns

| | 28 (AppFigures) | 86 (finance-kit) | 87 (this) |
|---|---|---|---|
| Ingress | Weekly analytics API → CSV | Vendor CSV drop | Daily FX API → nested JSON |
| Staging shape | Flat CSV columns | Flat CSV + hash cols | Nested `rates[]` record |
| Trusted load | APPEND + dbt | In-DAG hash-delta APPEND | UNNEST + EUR cross-rate APPEND |
| Hard part | Four report grains + dbt barrier | Ten-entity fan-out + hash dedupe | Nested land + dual-base FX factors + timestamp skip |

## Category

`utilities/87-open-exchange-rates-fx-land/`
