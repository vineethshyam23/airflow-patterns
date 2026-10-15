# Pattern 76: Odoo DWH full extract (Postgres → staging → dbt)

Primary inbound Odoo pipeline for the warehouse. Pulls a large table set
from the Odoo Postgres replica into GCS/BigQuery staging
(`WRITE_TRUNCATE`), then fans out domain dbt Cloud jobs and a final
datacore view refresh.

Production is ~105 tables / ~25 dbt jobs / ~350 tasks. This folder ships
a **representative cross-domain subset** that preserves the orchestration
contracts that actually mattered in production: parse-time TaskGroup,
replica throttle, local-disk upload, chunked junction tables, and
dagrun/dbt timeouts.

Distinct from pattern 15 (helpdesk-only Postgres pull) and from the
outbound / reverse family (06–14, 60, 72). Those DAGs read trusted
tables this spine feeds.

Source (read-only):
- `dags/etl_odoo_data_import_DWH.py`
- `dags/horeca_digital/odoo_data_export.py`
- `dags/horeca_digital/utils/odoo_utils.py` (`connect_odoo_postgres` only)

## Files

| File | Role |
|------|------|
| `dag_odoo_dwh_import.py` | Composer DAG: extract TaskGroup → pause → parallel dbt → datacore |
| `odoo_data_export.py` | `OdooDataPull` — lazy Postgres, NDJSON / 50k chunks, subset pulls |
| `odoo_postgres.py` | PgBouncer-safe `connect_odoo_postgres` |
| `schema_json/` | Sample BQ load schemas (crm_lead, sale_order, tax rel, mail) |
| `BUSINESS_CASE.md` | Why a full daily spine beside domain exports |
| `ARCHITECTURE.md` | Components + Mermaid diagram |
| `DATA_FLOW.md` | Stages, throttles, failure modes |

## Quick start

```bash
python -c "import ast; ast.parse(open('odoo_postgres.py').read())"
python -c "import ast; ast.parse(open('odoo_data_export.py').read())"
python -c "import ast; ast.parse(open('dag_odoo_dwh_import.py').read())"
```

To run for real you need Odoo Postgres Variables (`odoo_dm_creds` /
`odoo_prod_creds`), rawzone schemas under `schema_json/`, and the
`dbt_odoo_*_job_id` Variables listed in the DAG. This folder is a
sanitized reference, not a deploy of the full ~105-table list.

## Sanitization notes

- GCP projects `hd-dwh-stream-*` → `dwh_project` / `dwh_project_dev`
- Buckets `hd-digital-dp-*-rawzone` → `data-platform-*-rawzone`
- Dataset `dwh_trusted_staging` → `staging`
- Variable `odoo_DM_creds` → `odoo_dm_creds`
- Hard-coded dbt Cloud job IDs → Airflow Variables (`dbt_odoo_*_job_id`)
- Brand-prefixed custom columns (`dish_*`) aliased to `platform_*`
- `sale_order.access_token` dropped from extract
- Notification emails / owner names → `dataops@example.com` / `data-platform`
- Package imports `horeca_digital.*` → local modules
- Table list truncated to 16 representative tables (production ~105)
- dbt fan-out truncated to 9 domain jobs + datacore (production 25)
- Sample schemas only for four tables; production has one JSON per folder

## Distinct from pattern 15

| | Pattern 15 | Pattern 76 |
|---|------------|------------|
| Scope | Helpdesk entities only | Multi-domain Odoo spine |
| Cadence | On-demand (`None`) | Daily 01:00 UTC |
| Orchestration | Per-table chain, one dbt job | TaskGroup + parallel dbt + datacore |
| Throttle | Default parallelism | `max_active_tasks` + `dagrun_timeout` |
| Special paths | Ticket delta + dims | mail rename, CRM log notes, 50k chunks |

## Category

`odoo_integration/76-odoo-dwh-full-extract/`
