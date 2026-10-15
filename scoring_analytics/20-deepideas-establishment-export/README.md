# Pattern 20: Deepideas establishment attribute export

Weekly hash-delta of establishment enrichment attributes (geo
densities, digitalisation, cuisine, menu mix) for active wholesale
buyers. BigQuery builds a one-row-per-customer snapshot; Avro bulk
ingest posts only new or changed `_keyhash` / `_rowhash` pairs.

Distinct from pattern 16 (peer purchase gaps) and pattern 17 (full
monthly market listing). This feed answers "what is this buyer's
establishment profile?" — not "where do they under-index on spend?"
and not "what is the public listing document?"

Source (read-only):
- `dags/etl_dana_deep_ideas_export.py`
- `dags/horeca_digital/dana_deepideas_establishment_export.py`
- `dags/horeca_digital/dana_deepideas_query.py` (Establishment class)

## Files

| File | Role |
|------|------|
| `establishment_queries.py` | "Today" snapshot INSERT (active buyers + enrichment joins) |
| `delta_queries.py` | Send / copy / soft-close hash-delta SQL |
| `establishment_export.py` | OAuth + Avro encode + chunked POST |
| `dag_deepideas_establishment_export.py` | Weekly Composer wiring |
| `BUSINESS_CASE.md` | Why attribute profile ≠ peer gaps ≠ market listing |
| `ARCHITECTURE.md` | Components + Mermaid diagram |
| `DATA_FLOW.md` | Stages, delta rules, failure modes |

## Quick start

```bash
python -c "import ast; ast.parse(open('establishment_queries.py').read())"
python -c "import ast; ast.parse(open('delta_queries.py').read())"
python -c "import ast; ast.parse(open('establishment_export.py').read())"
python -c "import ast; ast.parse(open('dag_deepideas_establishment_export.py').read())"
python establishment_queries.py
python delta_queries.py
python establishment_export.py
```

Needs refined wholesale customer / establishment / menu tables,
Composer BigQuery connection `bigquery_default`, and event-API OAuth
Variables plus a registered schema id. This folder is a sanitized
reference, not a deploy.

## Sanitization notes

- GCP projects `hd-dwh-stream-*` → `dwh_project` / `dwh_project_dev`
- Datasets `dwh_refined` / `dwh_trusted_staging` → `refined` / `staging`
- `metro_id` → `wholesale_id`; `mcc_distance_*` → `store_distance_*`
- `analytical_mcc_*` / `mcc_geo_*` → `analytical_wholesale_*` / `wholesale_geo_*`
- Rating vendor columns generalized (`rating_a` / `rating_b` / …)
- Event API host / schema ids / OAuth Variable names generalized
- Real notification emails → `dataops@example.com`
- Owner / author names removed
- Package imports `horeca_digital.*` → local modules
- Avro schema parsed once per send (production re-parsed per row)
- Sibling feeds (gaps_category / gap_ingredients) left for later patterns;
  production DAG looped them in one graph — this sample isolates
  establishment

## Distinct from patterns 16 / 17

| | Pattern 16 | Pattern 17 | Pattern 20 |
|---|------------|------------|------------|
| Question | Peer spend under-index | Public listing document | Buyer establishment profile |
| Cadence | Daily compute + optional weekly export | Monthly full load | Weekly hash-delta |
| Grain | Category gap rows | Listing row per place | One row per wholesale_id |
| Payload | Flat category comparison | Wide geo/contact/ratings | Densities + digitalisation + menu mix |

## Category

`scoring_analytics/20-deepideas-establishment-export/`
