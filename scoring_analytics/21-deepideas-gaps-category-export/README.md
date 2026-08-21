# Pattern 21: Deepideas main-category gaps export

Weekly hash-delta of product main categories that show up on an
active buyer's menu (via ingredient mapping) but have no wholesale
purchase revenue in the last year. BigQuery builds the gap snapshot;
Avro bulk ingest posts only new or changed `_keyhash` / `_rowhash`
pairs.

Distinct from pattern 16 (peer spend under-index), patterns 12/14
(ranked menu-gap opportunities), and pattern 20 (establishment
attribute profile). This feed answers "which assortment categories
does the menu imply that we never sold them?" — not "where do they
under-index vs peers?" and not "what is the buyer's establishment
profile?"

Source (read-only):
- `dags/etl_dana_deep_ideas_export.py`
- `dags/horeca_digital/dana_deepideas_gaps_category_export.py`
- `dags/horeca_digital/dana_deepideas_query.py` (GapsCategory class)

## Files

| File | Role |
|------|------|
| `gaps_category_queries.py` | "Today" snapshot INSERT (menu→ingredient→category anti-join to revenue) |
| `delta_queries.py` | Send / copy / soft-close hash-delta SQL |
| `gaps_category_export.py` | OAuth + Avro encode + chunked POST |
| `dag_deepideas_gaps_category_export.py` | Weekly Composer wiring |
| `BUSINESS_CASE.md` | Why category gaps ≠ peer gaps ≠ menu ranking ≠ attrs |
| `ARCHITECTURE.md` | Components + Mermaid diagram |
| `DATA_FLOW.md` | Stages, delta rules, failure modes |

## Quick start

```bash
python -c "import ast; ast.parse(open('gaps_category_queries.py').read())"
python -c "import ast; ast.parse(open('delta_queries.py').read())"
python -c "import ast; ast.parse(open('gaps_category_export.py').read())"
python -c "import ast; ast.parse(open('dag_deepideas_gaps_category_export.py').read())"
python gaps_category_queries.py
python delta_queries.py
python gaps_category_export.py
```

Needs refined wholesale customer / transaction / article tables,
foodgraph menu→recipe→ingredient mappings, Composer BigQuery
connection `bigquery_default`, and event-API OAuth Variables plus a
registered schema id. This folder is a sanitized reference, not a
deploy.

## Sanitization notes

- GCP projects `hd-dwh-stream-*` / Vertex foodgraph projects → `dwh_project`
- Datasets `dwh_refined` / `dwh_trusted_staging` → `refined` / `staging`
- Foodgraph ACC datasets → `foodgraph` / `foodgraph_preprocessed`
- `metro_id` → `wholesale_id`; `mge_main_cat_*` → `product_main_cat_*`
- `analytical_mcc_*` → `analytical_wholesale_*`
- Event API host / schema ids / OAuth Variable names generalized
- Real notification emails → `dataops@example.com`
- Owner / author names removed
- Package imports `horeca_digital.*` → local modules
- Avro schema parsed once per send (production re-parsed per row)
- Production double-dot path typo on extracted-ingredients table
  corrected to a single dataset path
- Sibling feeds (establishment / gap_ingredients) left as separate
  patterns; production DAG looped them in one graph

## Distinct from patterns 12 / 14 / 16 / 20

| | 12/14 | 16 | 20 | 21 (this) |
|---|-------|----|----|-----------|
| Question | Ranked menu opportunities | Peer spend under-index | Buyer establishment profile | Menu-implied category with zero purchase |
| Cadence | Export batches | Daily compute + optional weekly | Weekly hash-delta | Weekly hash-delta |
| Grain | Opportunity rows | Category gap vs peers | One row per wholesale_id | wholesale_id × product_main_cat |
| Filter | Ranking / segment | Peer comparison | Active buyers | `revenue IS NULL` anti-join |

## Category

`scoring_analytics/21-deepideas-gaps-category-export/`
