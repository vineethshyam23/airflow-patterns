# Pattern 80: HD export → trusted MCC parallel materialization

Daily Composer DAG that fans out one copy task per country, lists
views in `export_{cnt}`, and materializes each into
`trusted_mcc.{cnt}_{name}` with `WRITE_TRUNCATE`. Soft view failures
return as plain XCom; an `ALL_DONE` prepare step aggregates them for
a Slack bot post.

Distinct from patterns 69/70 (wholesale NL MCC API + HubSpot) and
pattern 67 (refined-zone SCD spine): this is cross-project BigQuery
view → table materialization with parallel country tasks and
aggregated Slack failure reporting.

Source (read-only):
- `dags/etl_hd_export_to_trusted_mcc.py`
- `dags/horeca_digital/hd_export_to_trusted_mcc.py`

## Files

| File | Role |
|------|------|
| `hd_export_to_trusted_mcc.py` | list/copy helpers, XCom flatten, Slack text |
| `dag_hd_export_to_trusted_mcc.py` | Parallel country DAG + SlackAPIPostOperator |
| `BUSINESS_CASE.md` | Why a separate daily materialization DAG |
| `ARCHITECTURE.md` | Components + Mermaid diagram |
| `DATA_FLOW.md` | Schedule, path, failure modes |

## Quick start

```bash
python -c "import ast; ast.parse(open('hd_export_to_trusted_mcc.py').read())"
python -c "import ast; ast.parse(open('dag_hd_export_to_trusted_mcc.py').read())"
```

To run for real you need read IAM on the export project datasets,
write on `dwh_project.trusted_mcc`, Slack app connection
`slack_data_platform_app`, and Variable `slack_dwh_ops_channel`.
This folder is a sanitized reference, not a deploy package.

## Sanitization notes

- GCP project `cf-dana-ats-hd-data-shari-g4` → `export_project`
- GCP project `hd-dwh-stream-1` → `dwh_project`
- Dataset `hd_export_*` → `export_*`; `dwh_trusted_mcc` → `trusted_mcc`
- Slack channel id → Airflow Variable template
- Owner / emails → `data-platform` / `dataops@example.com`
- Ticket ids / author names removed from module headers
- Package import `horeca_digital.hd_export_to_trusted_mcc` → local
- Inline `google.cloud.bigquery` import moved to module top
- `print` → `logging`; Slack emoji markers removed from message text
- Skip-view name generalized (`bel_hd_rt_private_label` →
  `bel_rt_private_label`)
- Added `max_active_runs=1` on the DAG constructor

## Category

`utilities/80-hd-export-to-trusted-mcc/`
