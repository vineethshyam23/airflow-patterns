# Pattern 16: Peer benchmarking gaps (multi-country + Avro export)

Daily BigQuery diamond per country builds peer purchase-gap tables
(topsellers → skeletons, establishments → transactions → gaps). An
optional weekly path hash-deltas a flat category slice and Avro-posts
it to a partner event API.

Distinct from patterns 12/14 (menu-gap opportunity ranking) and
pattern 04 (FBO/NBO scoring). This pattern answers "where does this
establishment under-index vs segment peers on category spend?"

Source (read-only):
- `dags/etl_benchmarking_gaps.py`
- `dags/horeca_digital/benchmarking_gaps_queries.py`
- `dags/horeca_digital/dana_deepideas_benchmarking_gaps_export.py`
- `dags/horeca_digital/dana_deepideas_query.py` (BenchmarkingGaps delta helpers)
- `dags/etl_dana_deep_ideas_export.py` (export wiring reference)

## Files

| File | Role |
|------|------|
| `benchmarking_gaps_queries.py` | Per-country SQL builders (topsellers, skeletons, establishments, transactions, gaps) |
| `dag_benchmarking_gaps.py` | Composer DAG: diamond of BQ InsertJobs per enabled ISO |
| `benchmarking_gaps_export.py` | OAuth + Avro encode + chunked POST |
| `delta_queries.py` | Today/yesterday hash-delta SELECT / UPDATE helpers |
| `BUSINESS_CASE.md` | Why peer gaps ≠ menu gaps |
| `ARCHITECTURE.md` | Components + Mermaid diagram |
| `DATA_FLOW.md` | Daily compute + optional weekly export |

## Quick start

```bash
python -c "import ast; ast.parse(open('benchmarking_gaps_queries.py').read())"
python -c "import ast; ast.parse(open('dag_benchmarking_gaps.py').read())"
python -c "import ast; ast.parse(open('benchmarking_gaps_export.py').read())"
python -c "import ast; ast.parse(open('delta_queries.py').read())"
python -c "import benchmarking_gaps_queries as bg; print(bg.benchmarking_topsellers_query('DE','dwh_project','staging','refined','2026-08-12')[:200])"
python delta_queries.py
```

To run for real you need per-country refined establishment / customer /
article / transaction tables, Composer BigQuery connection
`bigquery_default`, and (for export) event-API OAuth Variables plus a
registered schema id. This folder is a sanitized reference, not a
deploy.

## Sanitization notes

- GCP projects `hd-dwh-stream-*` → `dwh_project` / `dwh_project_dev`
- Datasets `dwh_refined` / `dwh_trusted_staging` → `refined` / `staging`
- `analytical_mcc_*` → `analytical_wholesale_*`
- `metro_id` → `wholesale_id` (Avro field names follow; category fields
  flattened to `main_cat_*` / `cust_cat_*` on the export contract)
- `cofg_relevant` → `analytics_relevant`
- Event API host / schema ids / OAuth Variable names generalized
- Real notification emails → `dataops@example.com`
- Owner names removed from DAG body
- Package imports `horeca_digital.*` → local modules
- Full market-specific Deepideas INSERT for "today" omitted; delta
  contract preserved in `delta_queries.py`

## Category

`scoring_analytics/16-benchmarking-gaps/`
