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
7. **Phase 1 fallback** — one Cloud Function + Swagger/OpenAPI pair from `dags/horeca_digital/cloud_functions/` (prefer `prd/`) → `api_integrations/`

## Skipped

_None yet._

## Blockers

### 2026-07-16 — GitLab source clone/auth failed (Phase 1 blocked)

Cloud runner has no Mac path and cannot read the production Airflow repo.

**Source attempted (READ ONLY):**
- GitLab: `hospitality-digital/datalogue/dwh/airflow2`
- HTTPS: `https://gitlab.com/hospitality-digital/datalogue/dwh/airflow2.git`
- SSH: `git@gitlab.com:hospitality-digital/datalogue/dwh/airflow2.git`

**Exact errors:**
- HTTPS (`GIT_TERMINAL_PROMPT=0`): `fatal: could not read Username for 'https://gitlab.com': terminal prompts disabled` (exit 128)
- SSH (BatchMode): `Permission denied (publickey)` then `fatal: Could not read from remote repository` (exit 128)
- API unauthenticated: `GET /api/v4/projects/hospitality-digital%2Fdatalogue%2Fdwh%2Fairflow2` → HTTP 404 `{"message":"404 Project Not Found"}`

**Environment:**
- No `GITLAB_TOKEN` / `GITLAB_PRIVATE_TOKEN` / deploy key present
- Optional local path `/Users/vineethshyam/Documents/Work/airflow2/` not present (expected on Cloud)

**Action taken:** Stopped. Did not invent a pattern. No new pattern folder shipped.

**Unblock:** Add a GitLab Project Access Token or Deploy Token (`read_repository`) to the Cursor Cloud automation secrets as `GITLAB_TOKEN`, then clone:

```bash
git clone --depth 1 --filter=blob:none --sparse \
  "https://oauth2:${GITLAB_TOKEN}@gitlab.com/hospitality-digital/datalogue/dwh/airflow2.git" \
  /tmp/airflow2-source
cd /tmp/airflow2-source
git sparse-checkout set dags/horeca_digital
```

Next candidate once unblocked: `posms_predict_product_category.py` → `ml_pipelines/02-product-category-prediction/`
