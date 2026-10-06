# Pattern 67: Daily refined-zone SCD spine

Focused subset of the overnight `etl_refined_zone` Composer DAG:
reservation-pinned BigQuery jobs, sequential analytical actuals,
hash-based SCD Type 2 (insert new `_keyhash|_rowhash`, deferrable
expire), and a dual conservative/progressive test-establishment
quarantine table.

Production source is ~4.7k lines. This folder ships the spine only —
not the MCC country fan-out, Food Graph mapping unions, Order refined
tables, or bundle-over-time arm.

Source (read-only):
- `dags/etl_refined_zone.py`
- `dags/horeca_digital/bq_reservation.py`

## Files

| File | Role |
|------|------|
| `dag_daily_refined_zone_scd_spine.py` | Schedule, actuals chain, hist insert/expire, test list |
| `reserved_bq_operator.py` | Night-ETL reservation pin helper + operator subclass |
| `BUSINESS_CASE.md` | Why a spine subset, not the monolith |
| `ARCHITECTURE.md` | Components + Mermaid diagram |
| `DATA_FLOW.md` | Paths A–D, hash contract, failure modes |

## Quick start

```bash
python -c "import ast; ast.parse(open('reserved_bq_operator.py').read()); ast.parse(open('dag_daily_refined_zone_scd_spine.py').read())"
```

To run for real you need Composer with BigQuery, a reservation id in
Variable `bq_night_etl_reservation`, project id in `dwh_gcp_project`,
refined actual views, staging hist views with `_keyhash`/`_rowhash`,
and trusted Hydra / Reservation Tool establishment tables. This folder
is a sanitized reference, not a deploy package.

## Sanitization notes

- GCP project `hd-dwh-stream-1` → Variable `dwh_gcp_project`
  (default `dwh_project`)
- Datasets `dwh_refined` / `dwh_trusted_staging` / `dwh_trusted_views`
  → `refined` / `trusted_staging` / `trusted_views`
- Reservation path → Variable `bq_night_etl_reservation`
- Emails → `dataops@example.com`; author names removed
- Company email domains / brand test strings in regex → generic
  `example.com` / `testcorp` / `sample restaurant` placeholders
- Policy-specific geo exclusion branches (e.g. country filters on ERP
  test rows) dropped — not part of the reusable pattern
- Odoo / Order UNION arms of the production test query omitted; Hydra +
  Reservation Tool dual labels keep the engineering contract
- Factory over five representative SCD entities instead of copy-pasted
  insert/update blocks for every hist table
- `max_active_runs=1` added (production left it unset)
- DAG id renamed to `etl_refined_zone_scd_spine` for clarity

## Distinct from patterns 01 / 46 / 61 / 64 / 65

| | 01 | 46 | 61 | 64/65 | 67 (this) |
|---|----|----|----|-------|-----------|
| Domain | Match pairs | Food Graph zone | Derived events | MAG month hist | Daily refined spine |
| SCD style | Builders | Zone publish | LAG → append | Calendar APPEND | Hash insert + expire |
| Test quarantine | — | — | — | — | Dual mode |
| BQ reservation | — | — | — | — | Pinned night ETL |

## Category

`sql_patterns/67-daily-refined-zone-scd-spine/`
