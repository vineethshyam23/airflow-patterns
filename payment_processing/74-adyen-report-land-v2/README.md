# Pattern 74: Adyen report land v2 (settlement / received / margins)

Daily Composer land of Adyen Customer Area report CSVs for a POS payment
partner: settlement details, received payment details, and platform
margins. One calendar day per run, per-file GCS objects (no in-worker
merge), `file_name` lineage on every staging row, schema JSON on the
rawzone, and `CREATE TABLE IF NOT EXISTS` so dbt sources survive empty days.

Distinct from pattern 03 (Adyen Management API terminal inventory +
write-back). This pattern is report download / drop-folder land into
staging, not merchant/store/terminal CRUD.

Source (read-only):
- `dags/etl_eijsink_adyen_reports_v2.py`
- `dags/horeca_digital/eijsink_adyen_reports_v2.py`
- `dags/schema_json/eijsink_adyen_v2_*.json`

## Files

| File | Role |
|------|------|
| `dag_adyen_report_land_v2.py` | Parallel report branches + margins drop + optional dbt |
| `adyen_report_land_v2.py` | CA HTTP client, probe/upload, file_name stamp, ensure tables |
| `schema_json/*.json` | BigQuery load schemas (include `file_name`) |
| `BUSINESS_CASE.md` | Why v2 dropped merge and stamped lineage |
| `ARCHITECTURE.md` | Components + Mermaid diagram |
| `DATA_FLOW.md` | Run order, ShortCircuit behaviour, failure modes |

## Quick start

```bash
python -c "import ast; ast.parse(open('adyen_report_land_v2.py').read())"
python -c "import ast; ast.parse(open('dag_adyen_report_land_v2.py').read())"
```

To run for real you need Adyen CA report credentials in Variable
`adyen_report_creds`, rawzone schema JSON objects, GCS connections for
DEV/PROD, an analytics drop bucket for margins, and an optional dbt Cloud
job id in `adyen_report_v2_dbt_job_id`. This folder is a sanitized
reference, not a deploy.

## Sanitization notes

- GCP projects `hd-dwh-stream-*` → `dwh_project` / `dwh_project_dev`
- Raw buckets → `dp_rawzone` / `dp_dev_rawzone`
- Analytics drop bucket → `dp-analytics-drop`
- Staging dataset `dwh_trusted_staging` → `trusted_staging`
- Company account `BooqPayments` → `PartnerPayments`
- Table / path prefix `eijsink_adyen_v2_*` → `adyen_report_v2_*`
- Creds Variable → `adyen_report_creds`
- Hard-coded dbt job id → Variable `adyen_report_v2_dbt_job_id` (empty skips)
- Emails → `dataops@example.com`; owner → `data-platform`
- Ticket IDs and personal names removed
- Package import `horeca_digital.*` → local `adyen_report_land_v2`
- Inline `BigQueryHook` import moved to module top

No credentials were in the source files; still, never commit real Adyen keys.

## Category

`payment_processing/74-adyen-report-land-v2/`
