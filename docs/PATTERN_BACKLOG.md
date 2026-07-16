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
7. **Phase 1 fallback** — Cloud Function + Swagger pair under `cloud_functions/` → `api_integrations/`

## Skipped

_None yet._

## Blockers

### 2026-07-16 (run 2) — Source still unavailable (cloud, not local)

- Automation prompt expects a **LOCAL** Mac Agents Window with `/Users/vineethshyam/Documents/Work/airflow2/` readable.
- This run executed as a **cloud** agent (`bc-561e577d-…`) with **no private worker / no host mount**.
- Preferred path missing; no `/Users`, `/Volumes`, or host bind mounts present.
- No private `airflow2` remote found under `vineethshyam23` (or known dish orgs via token).
- Per rules: **do not invent patterns**. No pattern shipped.
- Prior same-day run: [PR #1](https://github.com/vineethshyam23/airflow-patterns/pull/1) (merged) recorded the first blocker.

### Unblock (required before next successful ship)

Pick one:

1. **Preferred**: Reconfigure automation [Daily Airflow Pattern Ship](https://cursor.com/automations/6cf8ca3f-8132-11f1-ba66-0e7d0216e441) to run on a **local / self-hosted private worker** on the Mac where `Documents/Work/airflow2` exists.
2. Mount or sync a **read-only** copy of `airflow2` into the cloud environment (at least `dags/horeca_digital/` and `archived/` + `cloud_functions/`).
3. Grant the automation GitHub token access to a **private read-only remote** of `airflow2`.

### Next candidate once source is readable

`posms_predict_product_category.py` → `ml_pipelines/02-product-category-prediction/`  
(Phase 1 DAG; if missing, fall back to one Cloud Function + Swagger pair.)
