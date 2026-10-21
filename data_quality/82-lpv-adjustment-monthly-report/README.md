# Pattern 82: LPV Adjustment monthly report

Monthly Composer DAG that pulls the previous calendar month from
`discovery.lpv_adjusted`, classifies Recurring vs Onetime, builds a
per-country CSV zip, and emails finance via SendGrid or SMTP.
Oversized zips offload to the Composer GCS bucket instead of failing
the send.

Distinct from pattern 32 (Invoice Radar): that job is a daily D-3
LPV vs invoice reconciliation with four Excel buckets. This job is a
month-close delivery — calendar month, country files, empty-month
no-op, attachment size guard.

Source (read-only, production Composer repo):
- `dags/etl_lpv_report.py`
- `dags/.../lpv_report/lpv_report.py`
- `dags/.../lpv_report_airflow/{config,report_generator,email_tasks}.py`
- `dags/.../email_delivery/` (shared SendGrid/SMTP helper)

## Files

| File | Role |
|------|------|
| `lpv_report.py` | BQ SQL, country zip, HTML payload |
| `config.py` | Airflow Variables → process env; pinned recipients |
| `report_generator.py` | Dynamic load of report module + staging dir |
| `email_tasks.py` | XCom → size offload → EmailDelivery |
| `email_delivery.py` | SendGrid / SMTP with attachment (zip MIME) |
| `dag_lpv_report.py` | start → generate → send → end |
| `BUSINESS_CASE.md` | Why finance runs this on the 5th |
| `ARCHITECTURE.md` | Components + Mermaid diagram |
| `DATA_FLOW.md` | Window, empty gate, offload path |

## Quick start

```bash
python -c "import ast; ast.parse(open('lpv_report.py').read())"
python -c "import ast; ast.parse(open('report_generator.py').read())"
python -c "import ast; ast.parse(open('dag_lpv_report.py').read())"
```

Needs Airflow Variables `lpv_report_config`, `env`, and either
`sendgrid_api_key` or `lpv_report_smtp_password`. Oversized
attachments also need `composer_bucket`. This folder is a sanitized
reference, not a deploy package.

## Sanitization notes

- GCP project IDs → `dwh_project` / `dwh_project_dev`
- Dataset / table → `discovery.lpv_adjusted`
- Owner / emails / SMTP hosts → example.com placeholders
- Ticket ids and internal GitLab notebook URLs removed from docs
- Internal package imports → local modules
- HTML emoji attachment icon removed
- Forced recipient list kept as example inboxes (same control pattern)

## Category

`data_quality/82-lpv-adjustment-monthly-report/`
