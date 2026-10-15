# Pattern 27: Offer Tool multi-table Cloud SQL SCD Type 2 ingest

Daily extract of ~15 Offer Tool (product-recommendation / field-sales)
MySQL tables into BigQuery trusted with Type 2 historization. Hashes
are computed in the Cloud SQL export SELECT so BigQuery only compares
`_keyhash` / `_rowhash` pairs.

Distinct from pattern 01 (matching-engine SCD on already-landed match
results) and pattern 10 (partner event-bus export of matching output).
This DAG owns the OLTP dump → raw zone → staging → trusted SCD loop.

Source (read-only):
- `dags/etl_customized_offering.py`
- `dags/horeca_digital/customized_offering_queries.py` (export queries only)

## Files

| File | Role |
|------|------|
| `export_queries.py` | MySQL SELECTs with MD5 key/row hashes + table list |
| `dag_customized_offering_scd.py` | Composer wiring: sequential exports, parallel SCD chains |
| `BUSINESS_CASE.md` | Why sequential dumps + hash-at-source + SCD2 |
| `ARCHITECTURE.md` | Components + Mermaid diagram |
| `DATA_FLOW.md` | Schedule, grain, failure modes |

## Quick start

```bash
python -c "import ast; ast.parse(open('export_queries.py').read())"
python -c "import ast; ast.parse(open('dag_customized_offering_scd.py').read())"
python -c "import export_queries as q; assert len(q.TABLE_NAMES)==len(q.EXPORT_QUERIES); print(len(q.TABLE_NAMES))"
```

Needs Cloud SQL Admin export IAM, a GCS export bucket the instance can
write to, raw-zone + schema JSON objects, and trusted / trusted_staging
datasets. This folder is a sanitized reference, not a deploy.

## Sanitization notes

- GCP projects / Cloud SQL instance names generalized
- Buckets `db-export-customized-offering-*` / raw zone → `db-export-offer-tool-prod` / `dwh-rawzone`
- Datasets `dwh_trusted*` → `trusted` / `trusted_staging`
- Table prefix `co_` → `ot_`; source tag `COP-Tool` → `OfferTool`
- Column `metro_id` → `wholesale_id` in details-update query
- Real notification emails → `dataops@example.com`
- Owner / author names removed; package import → local `export_queries`
- Zone / Elasticsearch DAG queries intentionally omitted (separate pattern)

## Distinct from patterns 01 / 10

| | 01 | 10 | 27 (this) |
|---|----|----|-----------|
| Question | Keep match history over time | Ship matches to partner bus | Land OLTP Offer Tool tables with history |
| Source | Matching service / staged results | Trusted matching output | Cloud SQL MySQL product DB |
| Cadence | Matching pipeline | Partner export schedule | Daily 06:15 UTC |
| Grain | Match key × version | Event payload | 15 tables × SCD2 rows |

## Category

`sql_patterns/27-customized-offering-scd-ingest/`
