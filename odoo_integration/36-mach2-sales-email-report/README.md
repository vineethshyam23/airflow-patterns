# Pattern 36: Mach2 Odoo sales Excel email report

Daily Composer DAG that queries refined Odoo sales tables in BigQuery,
builds up to five Excel workbooks (activations, cancellations, sales
channels, POS activation, POS cancellation), and emails each audience
via SendGrid or SMTP.

Distinct from pattern 32 (Invoice Radar): that job reconciles LPV vs
posted invoices for finance control. Mach2 is an operational sales /
POS lifecycle pack for partner ops — product codes, fiscal IDs, and
CRM matching keys, not discrepancy buckets.

Source (read-only, production Composer repo):
- `dags/etl_mach2_report.py`
- `dags/.../mach2_report/report.py` + `templates/`
- `dags/.../mach2_report_airflow/{config,report_generator,email_tasks}.py`
- `dags/.../email_delivery/` (shared SendGrid/SMTP helper)

## Files

| File | Role |
|------|------|
| `report.py` | BQ SQL, Excel formatting, email payload builder |
| `templates/` | HTML email bodies per report |
| `config.py` | Airflow Variables → process env |
| `report_generator.py` | Dynamic load of report module + staging dir |
| `email_tasks.py` | XCom → EmailDelivery |
| `email_delivery.py` | SendGrid / SMTP with attachment |
| `dag_mach2_report.py` | start → generate → send → end |
| `BUSINESS_CASE.md` | Why ops runs this daily |
| `ARCHITECTURE.md` | Components + Mermaid diagram |
| `DATA_FLOW.md` | Reports, windows, failure modes |

## Quick start

```bash
python -c "import ast; ast.parse(open('report.py').read())"
python -c "import ast; ast.parse(open('dag_mach2_report.py').read())"
```

Needs Airflow Variables `mach2_report_config` (JSON, no secrets),
`mach2_report_smtp_password` or `sendgrid_api_key`, and `env`. This
folder is a sanitized reference, not a deploy package.

## Sanitization notes

- GCP projects → `dwh_project` / `dwh_project_dev`
- Datasets `dwh_refined_sales` / `dwh_refined` → `refined_sales` / `refined`
- Partner CRM matching table → `partner_crm_matching_ids`
- Fiskaltrust production UUIDs → placeholder UUIDs
- Owner / emails / SMTP host → `data-platform` / `example.com`
- Package imports `horeca_digital.*` → local modules
- Branding in email shell → Platform Analytics
- Odoo custom column prefixes (`dish_*`) kept — they are the warehouse
  schema contract, not marketing copy
- Product CASE mappings retained as engineering examples; display
  brand strings softened where they were pure branding

## Category

`odoo_integration/36-mach2-sales-email-report/`
