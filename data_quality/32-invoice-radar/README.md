# Pattern 32: Invoice Radar (LPV vs invoice reconciliation)

Daily Composer DAG that reconciles expected subscription revenue
(LPV) against posted invoices, classifies discrepancies into four
buckets, optionally truncate-loads a BI snapshot table, and emails
finance a 4-sheet Excel plus HTML summary via SendGrid or SMTP.

Distinct from pattern 08 (Odoo WSL invoice dual export) and pattern
11 (payment KYC): this is an internal revenue-control report, not a
partner event-bus feed. The interesting engineering is the D-3
billing window, reason enrichment SQL, and the generate→XCom→send
split so SMTP failures do not re-query BigQuery.

Source (read-only, production Composer repo):
- `dags/etl_invoice_radar.py`
- `dags/.../invoice_radar/invoice_radar.py`
- `dags/.../invoice_radar/invoice_radar_alert.html`
- `dags/.../invoice_radar_airflow/{config,report_generator,email_tasks}.py`
- `dags/.../email_delivery/` (shared SendGrid/SMTP helper)

`invoice_ai_data_import.py` (AlloyDB OCR tables → BQ) is a different
job with hardcoded connection material in source — not included here.

## Files

| File | Role |
|------|------|
| `invoice_radar.py` | BQ SQL, bucket filters, Excel + HTML payload |
| `invoice_radar_alert.html` | Email body template (`{{ placeholders }}`) |
| `config.py` | Airflow Variables → process env |
| `report_generator.py` | Dynamic load of report module + staging dir |
| `email_tasks.py` | XCom → EmailDelivery |
| `email_delivery.py` | SendGrid / SMTP with attachment |
| `dag_invoice_radar.py` | start → generate → send → end |
| `BUSINESS_CASE.md` | Why finance runs this daily |
| `ARCHITECTURE.md` | Components + Mermaid diagram |
| `DATA_FLOW.md` | Window, buckets, failure modes |

## Quick start

```bash
python -c "import ast; ast.parse(open('invoice_radar.py').read())"
python -c "import ast; ast.parse(open('report_generator.py').read())"
python -c "import ast; ast.parse(open('dag_invoice_radar.py').read())"
```

Needs Airflow Variables `invoice_radar_config`, `env`, and either
`sendgrid_api_key` or `invoice_radar_smtp_password`. This folder is a
sanitized reference, not a deploy package.

## Sanitization notes

- GCP project IDs → `dwh_project` / `dwh_project_dev` (config override)
- Discovery / product / sales / BI dataset names generalized
- LPV / ERP asset / pricing table names generalized
- Product family labels → POS / PAYMENTS / LEGACY_SUITE
- Owner / emails / SMTP hosts → example.com placeholders
- Internal package imports → local modules
- HTML brand marks generalized; ticket id → `platform-4499`
- Dev recipient override kept as a single example inbox

## Category

`data_quality/32-invoice-radar/`
