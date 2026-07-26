# Pattern 12: Ranked menu-gaps export to partner event bus

Monthly pipeline that refreshes per-country ranked menu-gap tables via
dbt Cloud, then Avro-encodes and bulk-posts to a partner event API.
Countries run sequentially; within each country, five parallel tasks
split rows with `FARM_FINGERPRINT … MOD N` and stream from BigQuery so
a fat market does not load entirely into worker memory.

Distinct from pattern 04 (FBO/NBO scoring hash-delta) and pattern 10
(matching-engine service rows). This pattern ships *ranked menu-gap
opportunities* under a sequential-country / parallel-batch concurrency
model.

Source (read-only):
- `dags/etl_dana_rex_menu_gaps_export.py`
- `dags/horeca_digital/dana_rex_menu_gaps_export.py`
- `dags/horeca_digital/dana_rex_menu_gaps_query.py`

## Files

| File | Role |
|------|------|
| `menu_gaps_query.py` | Per-country SELECT + D-1 / full-load helper |
| `menu_gaps_export.py` | Hash-partition query, OAuth, Avro, chunked POST |
| `dag_rex_menu_gaps_export.py` | Composer DAG: dbt → sequential countries × parallel batches |
| `BUSINESS_CASE.md` | Why sequential countries + hash batches |
| `ARCHITECTURE.md` | Components + Mermaid diagram |
| `DATA_FLOW.md` | Run order, idempotency, failure modes |

## Quick start

```bash
python -c "import ast; ast.parse(open('menu_gaps_query.py').read())"
python -c "import ast; ast.parse(open('menu_gaps_export.py').read())"
python -c "import ast; ast.parse(open('dag_rex_menu_gaps_export.py').read())"
python menu_gaps_query.py    # prints a SQL prefix
python menu_gaps_export.py   # Avro schema parse + partitioned SQL smoke
```

To run for real you need the refined per-country tables, the dbt Cloud
job id in `dbt_job_menu_gaps_ranked_export`, event-API OAuth Variables,
and a registered schema id. This folder is a sanitized reference, not
a deploy.

## Sanitization notes

- GCP projects `hd-dwh-stream-*` → `dwh_project` / `dwh_project_dev`
- Dataset / tables `dwh_refined.dana_rex_menu_gaps_{cc}` →
  `refined.menu_gaps_ranked_{cc}`
- Product / partner / wholesale brand names generalized
- Column renames: `metro_*` / `art_*` / `accountId` →
  `wholesale_*` / `article_*` / `account_id` (Avro field names follow)
- Event API host / schema ids / OAuth Variable names generalized
- Real notification emails → `dataops@example.com`
- Owner / author names and internal ticket ids removed from DAG body
- Package imports `horeca_digital.*` → local modules
- dbt job id moved to Airflow Variable `dbt_job_menu_gaps_ranked_export`
- `max_active_runs` on DAG constructor (not only default_args)
- Streaming + single schema parse + 401-with-payload retry preserved
  from the production module (already stronger than older siblings)

## Category

`scoring_analytics/12-rex-menu-gaps-export/`
