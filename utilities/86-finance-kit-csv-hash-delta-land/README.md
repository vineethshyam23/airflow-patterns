# Pattern 86: Finance-kit multi-entity CSV land + hash-delta

Daily Composer fan-out over ten finance-kit CSV entities. Vendor drops
dated semicolon CSVs into a landing bucket; each entity runs the same
chain: list → copy to date-partitioned rawzone → staging TRUNCATE →
trusted APPEND of new `(_keyhash|_rowhash)` pairs → delete landing.

This is the *multi-entity land + in-DAG hash dedupe* pattern. It is
not SEO NDJSON land (pattern 25) and not POS HMAC store-details
ingest (pattern 35).

Source (read-only):
- `dags/etl_liquikit.py`

## Files

| File | Role |
|------|------|
| `finance_kit_entities.py` | Entity list + hash-delta INSERT SQL builder |
| `dag_finance_kit_land.py` | Ten parallel operator chains |
| `BUSINESS_CASE.md` | Why land+hash in Composer instead of dbt SCD |
| `ARCHITECTURE.md` | Components + Mermaid diagram |
| `DATA_FLOW.md` | Paths, macros, failure modes |

## Quick start

```bash
python -c "import ast; ast.parse(open('finance_kit_entities.py').read())"
python -c "import ast; ast.parse(open('dag_finance_kit_land.py').read())"
```

Needs `google_cloud_default` / `bigquery_default`, Variables for
project + buckets + datasets, schema objects at
`schema_json/finance_kit_{entity}.json` on the load bucket, and the
dated landing CSVs. This folder is a sanitized reference, not a
deploy package.

## Sanitization notes

- Product / DAG `liquikit` / `LiquiKit` → `finance_kit` / `FinanceKit`
- Landing bucket `hd-digital-dp-landingzone-datalogue` → Variable `landingzone_bucket`
- Raw bucket `hd-digital-dp-rawzone` → Variable `rawzone_bucket`
- GCP project `hd-dwh-stream-1` → Variable `dwh_project_id` (`dwh_project`)
- Datasets `dwh_trusted_staging` / `dwh_trusted` → `trusted_staging` / `trusted`
- Schema objects `liquikit_*.json` → `finance_kit_*.json`
- Owner / emails → `data-platform` / `dataops@example.com`
- Contrib GCS operators → current `providers.google` transfers
- Hash-delta SQL extracted to `finance_kit_entities.py`
- Entity stems (`bwa`, `umsatz`, …) kept — they are the vendor file contract

## Distinct from nearby patterns

| | 25 (SEO NDJSON) | 35 (HMAC CSV) | 86 (this) |
|---|---|---|---|
| Ingress | Vendor dump + four-prefix archive | Signed API fetch + CSV repair | Dropped dated CSVs |
| Entities | One listing table | One store-details table | Ten parallel entity chains |
| Trusted load | dbt after staging | dbt after staging | In-DAG hash-delta APPEND |
| Hard part | gunzip / md5 / archive | HMAC + column repair | Fan-out + key\|row hash dedupe + delete-after-append |

## Category

`utilities/86-finance-kit-csv-hash-delta-land/`
