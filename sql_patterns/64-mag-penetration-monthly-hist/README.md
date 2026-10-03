# Pattern 64: MAG penetration monthly historization

Composer DAG that freezes prior-month MAG penetration rates into an
append-only hist table on the **2nd of each month**: country actuals
first, then a corporate rollup row computed as a **delta against the
prior month's hist**.

Distinct from pattern 24 (outbound Avro export of the same hist), and
from zone rebuilds in patterns 46 / 48 / 62. This pattern owns the
*month-grain WRITE_APPEND + corp-delta* contract those consumers read.

Source (read-only):
- `dags/etl_refined_zone_2nd_of_month.py`

## Files

| File | Role |
|------|------|
| `dag_mag_penetration_hist.py` | Schedule, sequential append tasks |
| `penetration_queries.py` | Country actuals + corp delta SQL builders |
| `BUSINESS_CASE.md` | Why 2nd-of-month hist, not daily zone |
| `ARCHITECTURE.md` | Components + Mermaid diagram |
| `DATA_FLOW.md` | Paths A–B, failure modes, siblings |

## Quick start

```bash
python -c "import ast; ast.parse(open('penetration_queries.py').read())"
python -c "import ast; ast.parse(open('dag_mag_penetration_hist.py').read())"
python penetration_queries.py
```

To run for real you need BigQuery access on the DWH project, the live
penetration view and customer-base table, and Airflow Variables
`mag_hist_dwh_project` (and optionally `mag_hist_bq_conn`). This folder
is a sanitized reference, not a deploy package.

## Sanitization notes

- GCP project `hd-dwh-stream-1` → Variable `mag_hist_dwh_project`
  (default `dwh_project`)
- Dataset `dwh_refined` → `refined`
- Tables: `hist_penetration_rates_mag_reporting` →
  `hist_penetration_rates_reporting`;
  `vw_penetration_rates_mag_reporting` →
  `vw_penetration_rates_reporting`;
  `hd_customer_base_establishment` →
  `platform_customer_base_establishment`
- Metric prefixes: `*_MCC` → `*_wholesale`, `*_HD` → `*_platform`;
  vendor / suite product flags generalized (`booq` → `vendor`,
  `dish` → `suite`); corporate country label `"HD"` → `"corp"`
- Emails → `dataops@example.com`; owner names removed
- `max_active_runs=1` kept; SQL moved into builders for readability
- Production `TriggerRule.ALL_DONE` on both tasks preserved and
  documented as a re-run risk

## Distinct from patterns 24 / 46 / 48 / 62

| | 24 | 46 / 48 / 62 | 64 (this) |
|---|----|--------------|-----------|
| Role | Export hist outbound | Zone rebuild / barrier | Freeze month-end hist |
| Write | Avro POST | CREATE OR REPLACE etc. | WRITE_APPEND |
| Cadence | monthly ship | daily / bi-monthly | 2nd of month |
| Corp logic | none | n/a | delta vs prior hist |

## Category

`sql_patterns/64-mag-penetration-monthly-hist/`
