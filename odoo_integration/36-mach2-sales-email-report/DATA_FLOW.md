# Data flow: Mach2 Odoo sales email report

## Inputs

| Source | Grain | Role |
|--------|-------|------|
| `refined_sales.odoo_res_partner` | establishment / company | Active partners, fiscal tokens, debitor numbers |
| `refined_sales.odoo_sale_order` | order | State, dates, close reason |
| `refined_sales.odoo_sale_order_line` | line / exploded qty | Products and quantities |
| `refined_sales.odoo_product_*` | product | Codes and localized names |
| `refined_sales.odoo_res_country` | country | ISO codes / names |
| `refined_sales.odoo_sale_order_close_reason` | reason | Filter renewals vs true cancels |
| `refined.partner_crm_matching_ids` | establishment | Partner ERP matching keys |
| `odoo_mail_tracking_value` (POS activation) | tracking | Yesterday's license / fiscal field changes |

## Report windows

| Report | Audience env | Selection idea |
|--------|--------------|----------------|
| Sales channels | `EMAIL_TO_REPORT_1` | Mach2 basic license active; channel SKU attached; order created yesterday |
| Activation add-ons | `EMAIL_TO_REPORT_2` | Existing POS customer; new add-on order yesterday |
| Cancellation add-ons | `EMAIL_TO_REPORT_2` | Add-on closed; end date yesterday; not a plan renewal |
| POS cancellation | `EMAIL_TO_REPORT_2` | `POS_L%` license closed yesterday |
| POS activation | `EMAIL_TO_REPORT_3` | New POS provisioning; mail-tracking + fiscal fields present |

Exact predicates live in `report.py`. The important operational
contract is **yesterday in the warehouse timezone**, not "last DAG
success."

## Pipeline steps

1. `load_mach2_environment` — Variables → `os.environ`; DEV recipient override.
2. `generate_mach2_reports` — for each spec: SQL → DataFrame → Excel (if rows) → HTML → payload dict.
3. XCom push of the payload list (paths, not file bytes).
4. `send_mach2_report_emails` — pull XCom; send each message; log and continue on per-report failure.

## Failure modes

| Failure | Behaviour | Why |
|---------|-----------|-----|
| BQ error on one spec | That report skipped; others continue | One bad join should not wipe the morning pack |
| Excel write error | That report skipped | Prefer missing attachment over a half-written xlsx |
| Empty DataFrame | HTML email, no attachment | Explicit "no rows" vs silent skip |
| SMTP / SendGrid error | Logged; next report still attempted | Partial delivery beats zero delivery |
| Missing config Variable | Task fails at generate | Fail loud before querying |

## Outputs

- Staging: `/home/airflow/gcs/data/mach2_reports/<run_id>/*.xlsx` (Composer)
- Email: one message per successful payload, optional CC from config
- No warehouse write — this pattern is read → Excel → mail only
