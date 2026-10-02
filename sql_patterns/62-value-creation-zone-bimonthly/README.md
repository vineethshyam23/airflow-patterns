# Pattern 62: Value Creation Zone bi-monthly refresh

Composer DAG that refreshes Value Creation Dashboard (VCD) source
tables on the **3rd and 8th** of each month: parallel CREATE OR REPLACE
into `trusted_staging.vcd_*`, a sync barrier, a short-TTL transaction
union, then PSM uplift stored procedures for prod and dev.

Distinct from pattern 46 (Food Graph refined zone) and pattern 48
(Offer Tool weekday-aware product publish). This pattern owns the
*dashboard staging + stored-proc barrier* contract.

Source (read-only):
- `dags/etl_value_creation_zone_3rd_and_8th_of_month_v2.py`

## Files

| File | Role |
|------|------|
| `dag_value_creation_zone.py` | Schedule, country loops, barrier, SP fan-out, status |
| `vcd_queries.py` | Per-country + global SQL builders, PSM CALL helpers |
| `BUSINESS_CASE.md` | Why bi-monthly + one barrier DAG |
| `ARCHITECTURE.md` | Components + Mermaid diagram |
| `DATA_FLOW.md` | Paths A–F, Austria quirks, failure modes |

## Quick start

```bash
python -c "import ast; ast.parse(open('vcd_queries.py').read())"
python -c "import ast; ast.parse(open('dag_value_creation_zone.py').read())"
python vcd_queries.py
```

To run for real you need BigQuery access on the DWH project, upstream
wholesale / refined / MAG tables, Airflow Variable `vcd_dwh_project`
(and optionally `vcd_bq_conn`), and the `get_psm_uplift_values_v2`
procedure deployed. This folder is a sanitized reference, not a deploy
package.

## Sanitization notes

- GCP project `hd-dwh-stream-1` → Variable `vcd_dwh_project` (default
  `dwh_project`)
- Datasets: `dwh_trusted_mcc` → `trusted_wholesale`;
  `dwh_trusted_staging` → `trusted_staging`;
  `dwh_refined*` / `dwh_discovery` → `refined` / `refined_innovation` /
  `discovery`
- `metro_*` / MCC table naming → `wholesale_*`
- Hospitality filter text anonymized (`horeca` → `hospitality`)
- Emails / Slack channel / webhook → `dataops@example.com` + log stub
- Author names removed; `max_active_runs=1` added
- Sibling PSM CSV land DAG (`*_psm.py`) and archived v1 external-project
  copy omitted on purpose
- Inline SQL extracted to `vcd_queries.py`

## Distinct from patterns 46 / 48

| | 46 | 48 | 62 (this) |
|---|----|----|-----------|
| Cadence | daily | daily / Wed widen | 3rd + 8th |
| Layer | refined analytics | product projects | dashboard staging |
| Barrier | partitioned history | stage publish | PSM stored proc |
| Extra | fan-in UNION ALL | SoftCircuit | TTL union + SP × env |

## Category

`sql_patterns/62-value-creation-zone-bimonthly/`
