# Pattern 06: Odoo helpdesk tickets daily event export

Daily pipeline that refreshes a refined Odoo Level-1 helpdesk model via
dbt Cloud, then pushes yesterday's ticket creates as Avro events to an
external ingest API for one market.

Source (read-only):
- `dags/etl_dana_odoo_helpdesk_tickets.py`
- `dags/horeca_digital/dana_odoo_helpdesk_ticket.py`

## Files

| File | Role |
|------|------|
| `helpdesk_tickets_query.py` | Yesterday's-creates SELECT builder |
| `helpdesk_tickets_export.py` | OAuth client, Avro encode, chunked bulk POST |
| `dag_helpdesk_tickets_export.py` | Composer DAG: dbt refresh → ingest |
| `BUSINESS_CASE.md` | Why date-delta helpdesk export beats full reload |
| `ARCHITECTURE.md` | Components + Mermaid diagram |
| `DATA_FLOW.md` | Run order, idempotency, failure modes |

## Quick start

```bash
python -c "import ast; ast.parse(open('helpdesk_tickets_query.py').read())"
python -c "import ast; ast.parse(open('helpdesk_tickets_export.py').read())"
python -c "import ast; ast.parse(open('dag_helpdesk_tickets_export.py').read())"
python helpdesk_tickets_query.py   # prints the send SELECT
```

To run for real you need the refined helpdesk table, a dbt Cloud job that
rebuilds it, and Airflow Variables for the event API OAuth + schema id.
This folder is a sanitized reference, not a deploy.

## Sanitization notes

- GCP projects `hd-dwh-stream-*` → `dwh_project` / `dwh_project_dev`
- Dataset/table `dwh_refined.dana_odoo_helpdesk_ticket` →
  `refined.odoo_helpdesk_ticket`
- Columns `metro_account_identifier` / `metro_id` →
  `account_identifier` / `customer_id`
- Event API host / schema ids / OAuth Variable names generalized
- Real notification emails → `dataops@example.com`
- Owner / author names removed
- Package import `horeca_digital.dana_odoo_helpdesk_ticket` → local modules
- Inline SQL moved into `helpdesk_tickets_query.py`
- Commented credential literals from the source DEV block were dropped
- Avro schema parse moved outside the per-row loop (source parsed every row)

## Category

`odoo_integration/06-helpdesk-tickets-export/`
