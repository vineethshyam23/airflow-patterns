# Pattern 40: Food Graph ML propagation

Composer DAG that moves Food Graph ML outputs across three GCP
projects: Vertex preprocessed tables → DWH gold/trusted, month-end
ranked menu gaps → recommender partition, payment-wallet match
results → dbt Cloud.

Distinct from pattern 02 (POS text classification), pattern 12
(downstream ranked-gaps Avro export), and pattern 17 (market-data
Avro export). This is the multi-project landing and ranking spine.

Source (read-only):
- `dags/etl_foodgraph.py`
- `dags/horeca_digital/foodgraph_queries.py`

## Files

| File | Role |
|------|------|
| `dag_foodgraph.py` | Gold, propagation, ShortCircuits, ranking copy, dbt |
| `foodgraph_queries.py` | Gold SQL, unnested gaps, ranked / non-wholesale builders |
| `BUSINESS_CASE.md` | Why one DAG owns the cross-project contract |
| `ARCHITECTURE.md` | Components + Mermaid diagram |
| `DATA_FLOW.md` | Daily vs month-end paths, failure modes |

## Quick start

```bash
python -c "import ast; ast.parse(open('foodgraph_queries.py').read())"
python -c "import ast; ast.parse(open('dag_foodgraph.py').read())"
python foodgraph_queries.py
```

To run for real you need BigQuery access on the Vertex, DWH, and
recommender projects, Variables for project ids / dbt job id /
on-demand flag, and the dbt Cloud provider (or accept the stub).
This folder is a sanitized reference, not a deploy.

## Sanitization notes

- GCP projects `hd-dwh-vertex-*` / `hd-dwh-stream-*` /
  `recommender-vertex-rex-*` → Variables + `vertex_ml_project` /
  `dwh_project` / `recommender_rex_project`
- Metro / MCC / DISH Pay / DANA naming → wholesale / payment-wallet /
  partner
- CRM account table prefixes kept as placeholders (`ger`, `fra`, …)
- Emails → `dataops@example.com`
- Hardcoded dbt job id → Variable `foodgraph_match_result_dbt_job_id`
- On-demand toggle → Variable `foodgraph_ondemand_enabled`
- Partition suffix uses `{{ ds_nodash }}` instead of parse-time `now()`
- REX copy waits on all country tasks (production only linked the last)
- Orphaned on-demand tasks dropped; market-data branch omitted (pattern 17)
- Custom reserved BQ operator → `BigQueryInsertJobOperator`
- Package import `horeca_digital.foodgraph_queries` → local module

## Category

`ml_pipelines/40-foodgraph-ml-propagation/`
