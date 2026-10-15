# Pattern 71: Dining Guide DQ-gated multi-env publish

Daily Composer valve: dbt builds the Dining Guide establishment mart,
Blake3 short-hashes ids into public `dine_id`, compares ~30 proportion
and average metrics against yesterday's consumer-app prod snapshot,
and only then WRITE_TRUNCATEs into configured Dining Guide BigQuery
projects. Failures page Slack and leave app tables untouched.

Distinct from Food Graph multi-project copy (#40), Invoice Radar (#32),
and refined SCD spine (#67): this is a **publish gate** against the
live app project, not warehouse SCD or a finance report.

Source (read-only):
- `dags/etl_refined_dish_dine.py`
- `dags/horeca_digital/sql/dishdine_full_with_ratings_attributes.sql`
  (contract stubbed; proprietary SQL not shipped)

## Files

| File | Role |
|------|------|
| `dining_guide_validation.py` | Blake3 hash, comparison SQL, fail-closed gate |
| `dag_dining_guide_dq_gated_publish.py` | dbt → hash → compare → branch → publish |
| `sql/dining_guide_with_ratings.sql` | Dev-only ratings table stub |
| `BUSINESS_CASE.md` | Why a Composer gate vs dbt tests alone |
| `ARCHITECTURE.md` | Components + Mermaid diagram |
| `DATA_FLOW.md` | Happy path and failure modes |

## Quick start

```bash
python -c "import ast; ast.parse(open('dining_guide_validation.py').read())"
python -c "import ast; ast.parse(open('dag_dining_guide_dq_gated_publish.py').read())"
```

Needs Composer with dbt Cloud, BigQuery cross-project write to Dining
Guide envs, Variables listed below, and Slack webhook conn. This folder
is a sanitized reference, not a deploy package.

### Variables

| Variable | Purpose |
|----------|---------|
| `dwh_gcp_project` / `PROJECT` | DWH project id |
| `dining_guide_dbt_job_id` | dbt Cloud job |
| `dining_guide_publish_targets` | JSON `[[stage, project], ...]` |
| `dining_guide_prod_project` | Yesterday's compare baseline |
| `dining_guide_ratings_targets` | Dev-only ratings publish list |
| `dining_guide_slack_conn` / `_channel` | Fail alert |
| `dining_guide_bq_conn` | BQ connection id |

## Sanitization notes

- Brand Dish Dine → Dining Guide; tables `dish_dine_*` → `dining_guide_*`
- GCP `hd-dwh-stream-1` / `dishdine-*-*` → Variables +
  `dining-guide-{dev,acc,stg,prod}`
- Datasets `dwh_trusted` / `dwh_monitoring` / `dwh_data` /
  `dwh_backup_trusted` → `trusted` / `monitoring` / `app_data` /
  `backup_trusted`
- `sfdc_*` → `crm_*`; `metro_customer` → `wholesale_customer`;
  `dish_*` product flags → `order_*` / `reservation_*` / `website_*`
- Slack `#dish_dine_data` → `#dining-guide-data`; emails →
  `dataops@example.com`; owner → `data-platform`
- Docstring claimed 10% / code used 15% — docs and gate use **15%**
- Production `stage_and_project` overwrite quirk removed; targets are
  Variable-driven (dev vs prod defaults)
- `max_active_runs=1` added
- Validation helpers extracted from the DAG file
- Full ~1k-line with-ratings SQL replaced by a contract stub
- Emoji removed from Slack message

## Category

`data_quality/71-dining-guide-dq-gated-publish/`
