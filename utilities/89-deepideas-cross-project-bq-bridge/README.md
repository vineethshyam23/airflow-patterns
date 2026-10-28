# Pattern 89: DeepIdeas / menu-engineering cross-project BQ bridge

Daily Composer DAG with a single `BigQueryToBigQueryOperator` that
copies `co_menus` from the menu-engineering GCP project refined dataset
into the main DWH trusted layer (`WRITE_TRUNCATE`).

This is the hand-off between two projects, not the land itself.
Pattern 55 owns VM Postgres → GCS → BQ inside the menu-engineering
project. Pattern 89 only republishes the refined table where DWH
consumers and IAM already live.

Distinct from pattern 68 (intra-DWH sales mart two-phase copy) and
pattern 81 (weekly multi-table `copy_jobs` registry): one table, one
operator, daily late-evening schedule, cross-project source fixed.

Source (read-only):
- `dags/etl_di_import.py`

## Files

| File | Role |
|------|------|
| `dag_deepideas_cross_project_bq_bridge.py` | ENV wiring + single BQ-to-BQ truncate copy |
| `BUSINESS_CASE.md` | Why a dedicated bridge DAG exists |
| `ARCHITECTURE.md` | Components + Mermaid diagram |
| `DATA_FLOW.md` | Schedule, paths, failure modes |

## Quick start

```bash
python -c "import ast; ast.parse(open('dag_deepideas_cross_project_bq_bridge.py').read())"
```

To run for real you need read IAM on `menu_eng_project.refined.co_menus`,
write on `dwh_project.trusted.co_menus`, Variable `env`, and the matching
GCP connection. This folder is a sanitized reference, not a deploy
package.

## Sanitization notes

- GCP project `deepideas-development` → `menu_eng_project` (Variable-overridable)
- GCP project `hd-dwh-stream-1` → `dwh_project` (dev → `dwh_project_dev`)
- Dataset `dwh_refined` → `refined`; `dwh_trusted` → `trusted`
- Table `co_menus` kept (generic menu grain name)
- Owner / emails → `data-platform` / `dataops@example.com`
- Author name and ticket noise removed from headers
- Unused `ShortCircuitOperator` / `date` imports dropped
- Destination project id made explicit (prod relied on connection default)
- Added DAG `tags`; kept `max_active_runs=1`, retries=1, 22:00 UTC schedule

## Category

`utilities/89-deepideas-cross-project-bq-bridge/`
