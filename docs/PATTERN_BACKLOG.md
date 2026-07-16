# Pattern Backlog

Tracking file for the daily Airflow pattern shipping automation.
Source of truth for Done / Next / Skipped is also mirrored in automation Memories.

## Done

| # | Pattern | Category | Source (airflow2) | Notes |
|---|---------|----------|-------------------|-------|
| 01 | Matching Engine SCD Type 2 | `sql_patterns/01-matching-engine-scd-type2/` | (shipped before backlog) | In repo |

## Also already in repo (not from daily automation priority queue)

| Pattern | Category | Notes |
|---------|----------|-------|
| Accounts / invoice load | `odoo_integration/01-accounts-invoice-load/` | Existing |
| Leads ingestion | `odoo_integration/02-leads-ingestion/` | Existing |
| Opportunities load | `odoo_integration/03-opportunities-load/` | Existing |
| Dynamic TaskGroups | `odoo_integration/04-dynamic-taskgroups/` | Existing |
| Connection management | `odoo_integration/05-connection-management/` | Existing |

## Next (priority order)

1. **ML pipeline** — `dags/horeca_digital/.../posms_predict_product_category.py` → `ml_pipelines/02-product-category-prediction/`
2. **Payment API** — `adyen_payment_terminal_integration.py` → `payment_processing/`
3. **Customer scoring** — `dana_scoring_query.py` → `scoring_analytics/`
4. **Salesforce** — one strong SFDC DAG → `salesforce_integration/`
5. **Odoo daily/incremental sync** — not a full migration dump (prefer something not already covered under `odoo_integration/`)
6. Other unique API/DAG patterns not yet covered

## Skipped

_None yet._

## Blockers

### 2026-07-16 — Source access failed

- Preferred local path `/Users/vineethshyam/Documents/Work/airflow2/` is **not available** in this cloud environment (macOS host path).
- No readable private clone/URL of `airflow2` found via `gh` / `git ls-remote` for known orgs (`vineethshyam23`, `vineethshyam`, `dish-digital`).
- Per automation rules: **do not invent patterns**. Run stopped without shipping a new pattern.
- **Unblock**: mount or clone a read-only copy of `airflow2` into the automation environment (or grant access to a private remote), focusing on `dags/horeca_digital/` and `dags/horeca_digital/archived/`.
