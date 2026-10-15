# Pattern 65: MAG sales / acquisitions monthly historization

Composer DAG that freezes prior-month MAG product-bundle sales and
acquisitions into append-only hist tables on the **1st of each month**,
and rebuilds cleaned CRM partner IDs for FR/RO matching engine in
parallel (`WRITE_TRUNCATE`).

Distinct from pattern 64 (penetration hist + corp delta on the 2nd) and
from pattern 24 (outbound Avro export of MAG hist). This pattern owns
the *sales / acquisitions WRITE_APPEND + sales_all_time carry-forward*
contract and the partner-ID regex rebuild those consumers read.

Source (read-only):
- `dags/horeca_digital/archived/etl_refined_zone_monthly.py`

## Files

| File | Role |
|------|------|
| `dag_mag_sales_acquisitions_hist.py` | Schedule, MAG chain + parallel clean |
| `hist_queries.py` | Sales + acquisitions SQL builders |
| `partner_id_queries.py` | FR/RO partner-ID cleaning SQL |
| `BUSINESS_CASE.md` | Why 1st-of-month hist + independent clean |
| `ARCHITECTURE.md` | Components + Mermaid diagram |
| `DATA_FLOW.md` | Paths A–C, failure modes, siblings |

## Quick start

```bash
python -c "import ast; ast.parse(open('hist_queries.py').read())"
python -c "import ast; ast.parse(open('partner_id_queries.py').read())"
python -c "import ast; ast.parse(open('dag_mag_sales_acquisitions_hist.py').read())"
python hist_queries.py
python partner_id_queries.py
```

To run for real you need BigQuery access on the DWH project, the live
acquisition views and wholesale customer tables, and Airflow Variables
`mag_sales_hist_dwh_project` (and optionally `mag_sales_hist_bq_conn`).
This folder is a sanitized reference, not a deploy package.

## Sanitization notes

- GCP project `hd-dwh-stream-1` → Variable `mag_sales_hist_dwh_project`
  (default `dwh_project`)
- Datasets: `dwh_refined` → `refined`; `dwh_trusted_mcc` →
  `trusted_wholesale`; `dwh_trusted_views` → `trusted_views`
- Tables / views: `hist_sales_MAG_reporting` → `hist_sales_reporting`;
  `hist_acquisitions_mag_reporting` → `hist_acquisitions_reporting`;
  `vw_mag_acquisitions_base` → `vw_acquisitions_base`;
  `vw_acquisitions_mag_reporting` → `vw_acquisitions_reporting`;
  `sfdc_establishment_clean_metro_id` →
  `crm_establishment_clean_partner_id`;
  `asfdc_establishment` → `crm_establishment`;
  `fra_hd_customer` / `rom_hd_customer` →
  `fra_wholesale_customer` / `rom_wholesale_customer`
- Fields: `Metro_Id__c` → `Partner_Id__c`; `metro_id` /
  `unique_metro_id` / `clean_metro_id` → `partner_id` /
  `unique_partner_id` / `clean_partner_id`
- Bundle labels: `dishPrem/Start/Ord/Res/Pos` →
  `suitePrem/Start/Ord/Res/Pos`
- Event filter `"Dish Days"` → `"Platform Days"`
- Emails → `dataops@example.com`; owner names removed
- Hash comments inside SQL converted to `--`
- Dead unused CTE `c` from production dropped; join logic preserved
- `max_active_runs=1` added (production lacked it)
- Intended schedule `15 7 1 * *` restored (archived source used `None`)
- Production `TriggerRule.ALL_DONE` + `retries=0` preserved and
  documented as re-run risk

## Distinct from patterns 24 / 64 / 01 / 10

| | 24 | 64 | 65 (this) |
|---|----|----|-----------|
| Role | Export hist outbound | Freeze penetration + corp delta | Freeze sales/acq + clean partner IDs |
| Write | Avro POST | WRITE_APPEND penetration | WRITE_APPEND sales/acq + TRUNCATE IDs |
| Cadence | monthly ship | 2nd of month | 1st of month |
| Cumulative | none | corp delta vs prior hist | sales_all_time carry-forward |

Matching-engine SCD (#01) and partner export (#10) consume the cleaned
partner IDs; they do not own this rebuild.

## Category

`sql_patterns/65-mag-sales-acquisitions-monthly-hist/`
