# Pattern 17: Establishment market-data monthly Avro export

Monthly full-load of per-country refined market listing tables to a
partner event bus. Countries run sequentially; inside each country
five hash partitions stream BQ → Avro → chunked POST.

Distinct from patterns 12/14 (ranked menu gaps) and pattern 16 (peer
spend gaps). This feed is the establishment *listing* contract —
identity, geo, contact, ratings, hours, social, topics — not a
purchase-gap score.

Source (read-only):
- `dags/etl_dana_dish_market_data_export.py`
- `dags/horeca_digital/dana_dish_market_data_export.py`
- `dags/horeca_digital/foodgraph_queries.py` (`dish_market_data_active_isocode_list`)

## Files

| File | Role |
|------|------|
| `countries.py` | Active ISO list (shared by DAG + export) |
| `market_data_export.py` | OAuth + Avro encode + hash-partition SELECT + POST |
| `dag_dish_market_data_export.py` | Composer DAG: sequential countries, parallel batches |
| `BUSINESS_CASE.md` | Why full monthly load beat row-hash delta |
| `ARCHITECTURE.md` | Components + Mermaid diagram |
| `DATA_FLOW.md` | Schedule, sharding, failure modes |

## Quick start

```bash
python -c "import ast; ast.parse(open('countries.py').read())"
python -c "import ast; ast.parse(open('market_data_export.py').read())"
python -c "import ast; ast.parse(open('dag_dish_market_data_export.py').read())"
python market_data_export.py
```

To run for real you need per-country
`refined.establishment_market_data_{cc}`, event-API OAuth Variables,
and a registered schema id. This folder is a sanitized reference, not
a deploy.

## Sanitization notes

- GCP projects `hd-dwh-stream-*` → `dwh_project` / `dwh_project_dev`
- Dataset / table `dwh_refined.dish_market_data_{cc}` →
  `refined.establishment_market_data_{cc}`
- `md_establishment_id` / `google_places_id` → `establishment_id` /
  `places_id` on the export contract
- Event API host / schema ids / OAuth Variable names generalized
- Real notification emails → `dataops@example.com`
- Owner names removed from DAG body
- Package imports `horeca_digital.*` → local `countries` / modules
- Hard-coded prod schema hex → Airflow Variable with placeholder default
- `max_active_runs` set on the DAG constructor (was in default_args)

## Category

`scoring_analytics/17-dish-market-data-export/`
