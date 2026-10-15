# Data flow: Odoo DWH full extract

## Stages

1. **Fetch** — Worker opens a lazy Postgres connection, runs the
   table-specific SELECT, writes NDJSON under
   `/tmp/odoo_extract/{safe_run_id}/`, `fsync`s, uploads via GCSHook to
   `gs://{rawzone}/odoo/{folder}/{ds}/...`.
2. **Load** — `GCSToBigQueryOperator` loads that URI into
   `staging.odoo_{folder}` with `WRITE_TRUNCATE` and the matching
   `schema_json` object.
3. **Barrier** — `pause` (`TriggerRule.ALL_DONE`) waits for every
   extract/load pair in the TaskGroup, including failures.
4. **Domain dbt** — Parallel `DbtCloudRunJobOperator` tasks (Variable-
   driven job IDs) rebuild trusted models per domain.
5. **Datacore** — After `pause_2`, one dbt job refreshes analytical
   `aodoo_*` views that sit on trusted.
6. **End** — Downstream DAGs should sensor on this DAG’s success, not
   on individual staging tables.

## Extract styles

| Style | Tables (subset) | Notes |
|-------|-----------------|-------|
| Full SELECT | `crm_stage`, `crm_tag*`, `res_country`, `product_*`, `helpdesk_team` | Small dims; truncate is fine |
| create/write lookback | `crm_lead`, `sale_order*`, `helpdesk_ticket`, `res_partner`, `ir_model_data` | Today + yesterday |
| SFDC mail join | `mail_message` → folder `mail_message_email_activities` | Filtered by `ir_model_data` module |
| CRM log notes | `mail_message_crm_log_notes` (logical) | `crm.lead` comments since cutover |
| 50k chunks | `account_tax_sale_order_line_rel` | Wildcard GCS load |

## Idempotency

- GCS object paths include `{{ ds }}` — re-runs for the same logical
  date overwrite the same objects.
- Staging is `WRITE_TRUNCATE` per table — landing is idempotent for a
  successful load.
- Trusted rebuilds are owned by dbt; this DAG does not MERGE into
  trusted directly.
- `max_active_runs=1` prevents overlapping spines from racing on the
  same staging tables.

## Operability notes

- Large tables can sit near the 300-minute fetch timeout; watch
  `sale_order_line` and mail extracts first.
- `ALL_DONE` on pause means dbt may run against a partially refreshed
  staging set — treat DAG green as “spine finished,” not “every table
  perfect.” Inspect failed `data_fetch_*` / `load_staging_*` tasks.
- Raising `ODOO_INCREMENTAL_LOOKBACK_DAYS` without a catch-up run plan
  widens cost and still misses gaps older than the window.
