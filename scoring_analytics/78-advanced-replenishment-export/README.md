# Pattern 78: Advanced Replenishment Avro export to partner event bus

Daily pipeline that streams predicted ingredient-quantity rows from
a Vertex / foodgraph preprocessed table to a partner event API, plus
monthly platform-POS customer and articles→ingredients reference
feeds on the 5th UTC. Quantity uses ten parallel
`FARM_FINGERPRINT … MOD N` tasks and streams from BigQuery so a fat
market does not load entirely into worker memory.

Distinct from pattern 12/14 (ranked menu-gap opportunities) and
pattern 53 (POS Intelligence *article* recommendations). This
pattern ships *predicted ingredient demand* and the join tables the
partner needs beside it.

Source (read-only):
- `dags/etl_dana_advanced_replenishment_export.py`
- `dags/horeca_digital/dana_advanced_replenishment_export.py`
- `dags/horeca_digital/dana_auth.py` (token stagger / retry helpers)

## Files

| File | Role |
|------|------|
| `event_api_auth.py` | Stagger + throttle-aware token retry for parallel batches |
| `advanced_replenishment_export.py` | Queries, OAuth, Avro, chunked POST (3 subjects) |
| `dag_advanced_replenishment_export.py` | Composer DAG: daily batches + monthly branch |
| `BUSINESS_CASE.md` | Why one DAG, hash batches, and a 5th-of-month gate |
| `ARCHITECTURE.md` | Components + Mermaid diagram |
| `DATA_FLOW.md` | Run order, idempotency, failure modes |

## Quick start

```bash
python -c "import ast; ast.parse(open('event_api_auth.py').read())"
python -c "import ast; ast.parse(open('advanced_replenishment_export.py').read())"
python -c "import ast; ast.parse(open('dag_advanced_replenishment_export.py').read())"
python advanced_replenishment_export.py   # Avro schema parse + SQL smoke
```

To run for real you need the preprocessed prediction tables, event-API
OAuth Variables, and three registered schema ids. This folder is a
sanitized reference, not a deploy.

## Sanitization notes

- GCP projects `hd-dwh-stream-*` / `hd-dwh-vertex-*` → `dwh_project` /
  `ml_project` (+ `_dev`)
- Datasets `foodgraph_acc_preprocessed` / `foodgraph_dev_preprocessed`
  → `foodgraph_preprocessed` / `foodgraph_preprocessed_dev`
- Column renames: `metro_id` → `wholesale_id`, `ing_proper_DE` →
  `ingredient_name_local`, `art_no` / `ing_id` → `article_no` /
  `ingredient_id`
- Partner / wholesale brand names and ticket ids removed
- Event API host / schema ids / OAuth Variable names generalized
- Real notification emails → `dataops@example.com`
- Package imports `horeca_digital.*` → local modules
- Hard-coded schema hex ids → Airflow Variables with placeholder defaults
- Token stagger / retry helpers preserved (critical for 10 parallel batches)

## Category

`scoring_analytics/78-advanced-replenishment-export/`
