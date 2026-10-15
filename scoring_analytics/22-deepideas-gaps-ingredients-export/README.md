# Pattern 22: Deepideas gap-ingredients export

Weekly hash-delta of ingredients that show up on an active buyer's
menu (via recipe mapping) but have no wholesale purchase revenue on
articles mapped to that ingredient in the last year. BigQuery builds
the gap snapshot; Avro bulk ingest posts only new or changed
`_keyhash` / `_rowhash` pairs.

Distinct from pattern 21 (category-level zero-purchase gaps), pattern
16 (peer spend under-index), patterns 12/14 (ranked menu-gap
opportunities), and pattern 20 (establishment attribute profile). This
feed answers "which ingredients does the menu imply that we never sold
them articles for?" — not "which assortment category is missing."

Source (read-only):
- `dags/etl_dana_deep_ideas_export.py`
- `dags/horeca_digital/dana_deepideas_gaps_ingredients_export.py`
- `dags/horeca_digital/dana_deepideas_query.py` (GapIngredients class)

## Files

| File | Role |
|------|------|
| `gaps_ingredients_queries.py` | "Today" snapshot INSERT (menu→recipe→ingredient anti-join to revenue) |
| `delta_queries.py` | Send / copy / soft-close hash-delta SQL |
| `gaps_ingredients_export.py` | OAuth + Avro encode + chunked POST |
| `dag_deepideas_gaps_ingredients_export.py` | Weekly Composer wiring |
| `BUSINESS_CASE.md` | Why ingredient gaps ≠ category gaps ≠ peer gaps |
| `ARCHITECTURE.md` | Components + Mermaid diagram |
| `DATA_FLOW.md` | Stages, delta rules, failure modes |

## Quick start

```bash
python -c "import ast; ast.parse(open('gaps_ingredients_queries.py').read())"
python -c "import ast; ast.parse(open('delta_queries.py').read())"
python -c "import ast; ast.parse(open('gaps_ingredients_export.py').read())"
python -c "import ast; ast.parse(open('dag_deepideas_gaps_ingredients_export.py').read())"
python gaps_ingredients_queries.py
python delta_queries.py
python gaps_ingredients_export.py
```

Needs refined wholesale customer / transaction tables, foodgraph
menu→recipe→ingredient mappings, Composer BigQuery connection
`bigquery_default`, and event-API OAuth Variables plus a registered
schema id. This folder is a sanitized reference, not a deploy.

## Sanitization notes

- GCP projects `hd-dwh-stream-*` / Vertex foodgraph projects → `dwh_project`
- Datasets `dwh_refined` / `dwh_trusted_staging` / `dwh_trusted` → `refined` / `staging` / `trusted`
- Foodgraph preprocessed datasets → `foodgraph_preprocessed`
- `metro_id` → `wholesale_id`; `mge_main_cat_id` → `product_main_cat_id`
- `analytical_mcc_*` → `analytical_wholesale_*`
- Event API host / schema ids / OAuth Variable names generalized
- Real notification emails → `dataops@example.com`
- Owner / author names removed
- Package imports `horeca_digital.*` → local modules
- Avro schema parsed once per send (production re-parsed per row)
- Sibling feeds (establishment / gaps_category) already shipped as
  patterns 20 / 21; production DAG looped them in one graph

## Distinct from patterns 12 / 14 / 16 / 20 / 21

| | 12/14 | 16 | 20 | 21 | 22 (this) |
|---|-------|----|----|----|-----------|
| Question | Ranked menu opportunities | Peer spend under-index | Buyer establishment profile | Menu-implied category with zero purchase | Menu-implied ingredient with zero purchase |
| Cadence | Export batches | Daily compute + optional weekly | Weekly hash-delta | Weekly hash-delta | Weekly hash-delta |
| Grain | Opportunity rows | Category gap vs peers | One row per wholesale_id | wholesale_id × product_main_cat | wholesale_id × ingredient × product_main_cat |
| Filter | Ranking / segment | Peer comparison | Active buyers | `revenue IS NULL` at category | `revenue IS NULL` at ingredient |

## Category

`scoring_analytics/22-deepideas-gaps-ingredients-export/`
