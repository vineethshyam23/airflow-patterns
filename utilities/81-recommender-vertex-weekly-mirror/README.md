# Pattern 81: Recommender Vertex weekly mirror

Weekly Composer DAG that treats every full-replace BigQuery copy as
one entry in a `copy_jobs` list: SELECT, destination dataset, table.
All jobs run in parallel after `start` with BATCH priority and
WRITE_TRUNCATE.

Two groups share the registry:

- Cross-project REX market potential → `refined`
- Intra-DWH curated SELECTs → `insights_share` (from
  `SHARE_QUERIES`)

Distinct from patterns 40/45 (Food Graph / Vertex PipelineJob) and
pattern 53 (POS Intelligence Avro export): this is a weekly mirror
registry, not ML submit or partner event publish. Distinct from
pattern 80 (daily country-list + Slack aggregate materialization).

Source (read-only):
- `dags/etl_recommender_vertex.py`
- `dags/dish_insights_share_queries.py`

## Files

| File | Role |
|------|------|
| `dag_recommender_vertex_weekly_mirror.py` | copy_jobs registry + parallel InsertJobs |
| `insights_share_queries.py` | SELECT-only map for insights_share |
| `BUSINESS_CASE.md` | Why one weekly registry DAG |
| `ARCHITECTURE.md` | Components + Mermaid diagram |
| `DATA_FLOW.md` | Schedule, paths, failure modes |

## Quick start

```bash
python -c "import ast; ast.parse(open('insights_share_queries.py').read())"
python -c "import ast; ast.parse(open('dag_recommender_vertex_weekly_mirror.py').read())"
```

To run for real you need read IAM on `rex_vertex_project` and the
DWH source datasets, write on `dwh_project.refined` and
`dwh_project.insights_share`, plus Variable `env` and the matching
GCP connection. This folder is a sanitized reference, not a deploy
package.

## Sanitization notes

- GCP project `hd-dwh-stream-1` → `dwh_project` (dev → `dwh_project_dev`)
- GCP project `recommender-vertex-rex-prod-57` → `rex_vertex_project`
- Dataset `dwh_refined` → `refined`; `dwh_dish_insights_share` → `insights_share`
- Dataset prefixes `dish_*` / `dwh_*` shortened where they were brand noise
- Table names `horeca_shg_potencjal` / `rj_art_potencial` →
  `horeca_shg_potential` / `article_potential` (spelling normalized)
- Owner / emails → `data-platform` / `dataops@example.com`
- Ticket ids / author names removed from headers and docstrings
- `DummyOperator` → `EmptyOperator` with Airflow 1.x fallback
- Removed deprecated `provide_context` from default_args
- Import `dish_insights_share_queries` → local `insights_share_queries`
- Added DAG `tags`; kept `max_active_runs=1`

## Category

`utilities/81-recommender-vertex-weekly-mirror/`
