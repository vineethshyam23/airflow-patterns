# Pattern 46: Food Graph refined multi-country zone

Composer DAG that builds the Food Graph **refined analytics** dataset:
16 parallel country chains, UNION ALL fan-in, a Dummy/EmptyOperator
sync barrier, then DAY-partitioned establishment transaction history.

Distinct from pattern 40 (cross-project ML gold / ranked-gaps copy)
and pattern 45 (Vertex PipelineJob submit). This pattern owns the
refined-layer fan-out / fan-in contract those jobs consume.

Source (read-only):
- `dags/etl_refined_foodgraph_zone.py`
- `dags/horeca_digital/foodgraph_queries.py`

## Files

| File | Role |
|------|------|
| `dag_refined_foodgraph_zone.py` | Country loops, loop1 barrier, fan-in, partitioned loads |
| `foodgraph_refined_queries.py` | Per-country + global SQL builders |
| `BUSINESS_CASE.md` | Why one DAG owns the barrier |
| `ARCHITECTURE.md` | Components + Mermaid diagram |
| `DATA_FLOW.md` | Paths A–D, failure modes |

## Quick start

```bash
python -c "import ast; ast.parse(open('foodgraph_refined_queries.py').read())"
python -c "import ast; ast.parse(open('dag_refined_foodgraph_zone.py').read())"
python foodgraph_refined_queries.py
```

To run for real you need BigQuery access on the DWH project, upstream
wholesale / card country tables, Airflow Variable `foodgraph_dwh_project`
(and optionally `foodgraph_bq_reservation`), and a GCP connection.
This folder is a sanitized reference, not a deploy package.

## Sanitization notes

- GCP project `hd-dwh-stream-1` → Variable `foodgraph_dwh_project`
  (default `dwh_project`)
- Dataset `dwh_refined_foodgraph` → `refined_foodgraph`
- Metro / MCC naming → wholesale / wholesale_card
- `ReservedBigQueryInsertJobOperator` → standard
  `BigQueryInsertJobOperator` + optional reservation Variable
- Emails / owners → `dataops@example.com` / `data-platform`
- Dead `foodgraph_transactions_split` PythonOperators and `print`
  debug in the country loop removed
- Nested visit JSON (addresses, phones) collapsed to the flat visit
  shape; COP masterdata drops contact columns
- Added `max_active_runs=1` (called out as missing in production docs)
- Wired post-fan-in edges that production left ambiguous; documented
  remaining root-level globals
- Package import `horeca_digital.foodgraph_queries` → local module

## Category

`sql_patterns/46-refined-foodgraph-zone/`
