# Pattern 70: Wholesale NL HubSpot reverse export

Manual Composer DAG that pushes three BigQuery discovery enrichment
tables (prospects, matched, dedupe pairs) to partner MCC HubSpot
endpoints in 5k-record OAuth2 batches with a shared `sessionid` per
task.

Distinct from pattern 69 (inbound MCC + CHD land), pattern 59
(Salesforce refined warehouse fan-out), and Avro event-bus exports
(10 / 11 / 53): full-table CRM reverse ETL, not land or bus publish.

Source (read-only):
- `dags/etl_makro_hubspot_export.py`
- `dags/horeca_digital/makro_customers_api.py` (export_* helpers only)

## Files

| File | Role |
|------|------|
| `wholesale_nl_hubspot_export.py` | OAuth client, queries, chunked POST |
| `dag_wholesale_nl_hubspot_export.py` | Manual DAG — three parallel tasks |
| `BUSINESS_CASE.md` | Why manual full-table CRM push |
| `ARCHITECTURE.md` | Components + Mermaid diagram |
| `DATA_FLOW.md` | Steps and failure modes |

## Quick start

```bash
python -c "import ast; ast.parse(open('wholesale_nl_hubspot_export.py').read())"
python -c "import ast; ast.parse(open('dag_wholesale_nl_hubspot_export.py').read())"
```

Needs Airflow Variables for MCC OAuth (`wholesale_nl_mcc_*`,
`mcc_oauth_*`), three HubSpot export URLs, `wholesale_nl_enrichment_snapshot`,
and `dwh_gcp_project` / `discovery_dataset`. This folder is a sanitized
reference, not a deploy package.

## Sanitization notes

- GCP projects `hd-dwh-stream-1` / `-dev` → Variables `dwh_project` /
  `dwh_project_dev`
- Dataset `dwh_discovery` → Variable `discovery_dataset`
- Brand Makro → Wholesale NL; table prefix `MakroNL` → `WholesaleNL`
- Payload keys `makro_cust_key` / `deepideas_id` / `eijsink_id` →
  `wholesale_cust_key` / `menu_eng_id` / `pos_vendor_id`
- Variable family `makroNL_hubspot_*` → `wholesale_nl_hubspot_*` /
  shared `wholesale_nl_mcc_*` OAuth with pattern 69
- Hardcoded `*_20250922` suffix → Variable
  `wholesale_nl_enrichment_snapshot`
- Owner / emails → `data-platform` / `dataops@example.com`
- Package import `horeca_digital.makro_customers_api` → local module
- Inbound land helpers omitted (pattern 69)
- API errors re-raise (production swallowed); unused DAG bucket locals
  dropped; prospects/matched consolidated behind shared SELECT
- Fixed `sessionid` range (`1**12` was always 1)

## Category

`utilities/70-wholesale-nl-hubspot-export/`
