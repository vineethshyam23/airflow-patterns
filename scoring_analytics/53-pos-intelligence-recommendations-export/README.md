# Pattern 53: POS Intelligence recommendations → partner event bus

Monthly full-load of Vertex / foodgraph POS Intelligence article
recommendations to a partner event ingest API. Countries run
sequentially; each streams BQ → Avro → chunked POST.

Distinct from patterns 12/14 (ranked menu gaps) and pattern 16 (peer
spend gaps). This feed is the *wholesale article recommendation*
contract — POS usage vs purchase gap, ranked articles — not a
menu-gap score.

Source (read-only):
- `dags/etl_dana_pos_intelligence_recommendations_export.py`
- `dags/horeca_digital/dana_pos_intelligence_export.py`

## Files

| File | Role |
|------|------|
| `pos_intelligence_export.py` | OAuth + typed SELECT + Avro encode + chunked POST |
| `dag_pos_intelligence_export.py` | Composer DAG: sequential countries, 4h timeout |
| `BUSINESS_CASE.md` | Why full monthly load + pilot-then-extend countries |
| `ARCHITECTURE.md` | Components + Mermaid diagram |
| `DATA_FLOW.md` | Schedule, typing, failure modes |

## Quick start

```bash
python -c "import ast; ast.parse(open('pos_intelligence_export.py').read())"
python -c "import ast; ast.parse(open('dag_pos_intelligence_export.py').read())"
python pos_intelligence_export.py
```

To run for real you need
`ml_project.foodgraph_preprocessed*.pos_article_final_recommendation_{CC}`,
event-API OAuth Variables, and a registered schema id. This folder is
a sanitized reference, not a deploy.

## Sanitization notes

- GCP projects `hd-dwh-stream-*` / Vertex project → `dwh_project` /
  `dwh_project_dev` / `ml_project`
- Dataset `foodgraph_*_preprocessed` → `foodgraph_preprocessed*`
- Table `pos_potential_metro_article_final_recommendation_{CC}` →
  `pos_article_final_recommendation_{CC}`
- Column renames: `metro_*` / `cust_no` / `art_*` / `var_tu_key` →
  `wholesale_*` / `customer_no` / `article_*` / `variant_tu_key`
- Event API host / schema ids / OAuth Variable names generalized
- Real notification emails → `dataops@example.com`
- Owner / ticket identifiers removed from DAG body
- Package imports `horeca_digital.*` → local module + local `batched`
- Hard-coded prod schema hex → Airflow Variable with placeholder default
- `max_active_runs` / `max_active_tasks` on the DAG constructor

## Category

`scoring_analytics/53-pos-intelligence-recommendations-export/`
