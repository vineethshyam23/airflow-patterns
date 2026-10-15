# Data flow: Salesforce refined daily aggregates

## Schedule

- Cron: `0 7 * * *` (07:00 UTC)
- `max_active_runs=1`, `catchup=False`
- Retries: 1 / 10-minute delay

## Run order

1. **start**
2. **Parallel stage A**
   - `create_orders_aggregated` → `refined_salesforce.orders_aggregated`
   - `create_reservations_aggregated` → `refined_salesforce.reservations_aggregated`
3. **stage_1** (`ALL_DONE`)
4. **Parallel stage B**
   - `create_subscription_billing_info`
   - `create_voucher_info`
   - `create_sfdc_odoo_export` (Odoo WSL × SFDC assets + `_rowhash`)
   - `odoo_wsl_invoice_lines_copy` (raw trusted view copy)
   - `app_login`
5. **stage_2** (`ALL_DONE`)
6. **pos_transactions_aggregated** — four-country POS union × POS licenses
7. **email** — completion notice (`ALL_DONE`)
8. **end**

## Output tables

| Task | Table | Grain |
|------|-------|-------|
| create_orders_aggregated | orders_aggregated | establishment × date × currency |
| create_reservations_aggregated | reservations_aggregated | country × SFID × date |
| create_subscription_billing_info | subscription_billing_info | establishment × subscription |
| create_voucher_info | voucher_info | voucher × country × merchant |
| create_sfdc_odoo_export | sfdc_odoo_export | Odoo invoice / SFDC asset revenue |
| odoo_wsl_invoice_lines_copy | odoo_wsl_invoice_lines | invoice line (raw copy) |
| app_login | app_login | establishment SFID |
| pos_transactions_aggregated | pos_transactions_aggregated | country × establishment × license × date |

## Idempotency

| Layer | Behaviour on re-run |
|-------|---------------------|
| All eight BQ tables | `WRITE_TRUNCATE` — always current snapshot |
| Email | Re-sends on re-run (`ALL_DONE`) |

## Failure modes

- **Sibling BQ failure with `ALL_DONE`** — stage markers and email can
  still fire. Downstream may read a mix of today's and yesterday's
  tables. Prefer `ALL_SUCCESS` if CRM cannot tolerate partial refresh.
- **Odoo / SFDC join mismatch** — `ProductBaseCode` vs product code
  drift produces missing revenue rows; check `_rowhash` coverage.
- **POS union cost** — four country scans in one job; slot spikes are
  expected. Keep POS last so cheaper tables finish first.
- **Hardcoded `bigquery_default`** — DEV project surprises until you
  wire env-specific conn ids (same footgun as pattern 58).
