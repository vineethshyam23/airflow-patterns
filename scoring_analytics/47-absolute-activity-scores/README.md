# Pattern 47: Absolute multi-channel activity scores

Monthly Composer DAG that builds establishment activity scores (0–3)
from a fan-in of product, CRM, and analytics channels. Base extracts
feed per-channel rolling tables; those FULL OUTER JOIN into a thresholded
score row, then a MoM transition table for customer-engagement reporting.

Distinct from pattern 04 (FBO/NBO partner export) and the Deepideas /
menu-gap family (12–22): this pattern answers "how engaged is this
establishment across our own product surface this month?" — not what to
sell them next.

Source (read-only):
- `dags/absolute_activityscores.py`

Related (not shipped here): production later wrapped the Odoo-adjusted
sibling in `dags/etl_activity_score_job.py` (dbt Cloud). That migration
is the natural next step once the SQL contract is stable — it is out of
scope for this Airflow-only pattern.

## Files

| File | Role |
|------|------|
| `dag_absolute_activity_scores.py` | 18 BQ InsertJobs + fan-in edges |
| `activity_score_queries.py` | SQL builders for every stage |
| `BUSINESS_CASE.md` | Why a 0–3 score lived in Composer first |
| `ARCHITECTURE.md` | Components + Mermaid diagram |
| `DATA_FLOW.md` | Base → channel → score → MoM paths |

## Quick start

```bash
python -c "import ast; ast.parse(open('activity_score_queries.py').read())"
python -c "import ast; ast.parse(open('dag_absolute_activity_scores.py').read())"
python -c "import activity_score_queries as q; print(q.query_as_absolute_base_wb_rt_event()[:200])"
python -c "import activity_score_queries as q; print(len(q.query_as_absolute_activity_score()))"
```

To run for real you need the refined channel bases, external KPI
threshold + country-type tables, Composer Variables
`activity_score_bq_project` / `activity_score_bq_conn`, and a BigQuery
connection. This folder is a sanitized reference, not a deploy package.

## Sanitization notes

- GCP project `hd-dwh-stream-1` → Variable `activity_score_bq_project`
  (default `dwh_project`)
- Datasets `dwh_refined` / `dwh_trusted` / `dwh_external` /
  `dwh_trusted_views` → `refined` / `trusted` / `external` /
  `trusted_views`
- Product brands (CMS / reservation / order / portal / mobile / POS)
  generalized; `hydra` → `cms`
- `booq_id` → `pos_vendor_id`; customer-base table renamed
- Region exclusion label generalized
- Emails / owners → `dataops@example.com` / `data-platform`
- SQL extracted from inline operator bodies into `activity_score_queries.py`
- Added `max_active_runs=1` (monthly append jobs should not overlap)
- MoM table renamed `RT_CE_activity_score` →
  `ce_activity_score_transitions`

## Category

`scoring_analytics/47-absolute-activity-scores/`
