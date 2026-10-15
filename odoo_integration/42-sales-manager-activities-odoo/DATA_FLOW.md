# Data flow: Field-sales activities → Odoo

## Daily path (03:00 UTC)

1. **start** — EmptyOperator.
2. **same_day_branch** — Compare current vs previous `DagRun.queued_at`
   calendar dates. Same day → `skip_pipeline` → end. Else continue.
3. **Per country** (FR, ES, DE, NL, RO, HU, IT, HR in parallel):
   1. **fetch_{CC}** — OAuth2 token; page activities for
      `[yesterday, today]` with `status=Completed`; write
      `/home/airflow/gcs/data/sam/{cc}/sam_response.json`.
   2. **copy_raw_{CC}** — Composer data object →
      `gs://{rawzone}/sam/{cc}/sam_response_{yyyymmdd}.json`.
   3. **stage_{CC}** — NDJSON →
      `{project}.trusted_staging.sales_manager_activities_{cc}`
      WRITE_APPEND, schema from `schema_json/sam_activities.json`.
4. **countries_done** — join.
5. **load_crm_product_mapping** — TRUNCATE reload of product map used
   by dbt / Odoo product resolution.
6. **dbt_sam_leads** — dbt Cloud job from Variable
   `sam_leads_dbt_job_id` (skipped when empty / provider missing).
7. **push_odoo_leads** — pattern 02 lead engine creates `crm.lead`.
8. **lead_monitoring_odoo** — warehouse vs Odoo counts → XCom.
9. **slack_notification_odoo** — MATCH / STUB / FAILED message.
10. **end** — `none_failed_min_one_success` so skip path can finish.

## Idempotency

- Same-day re-queue skips extract (branch), not a GCS existence check.
- Staging is APPEND: intentional clear/re-run on a new calendar day
  adds another window; same-day clear is blocked by the branch.
- Product mapping is full refresh each successful run.

## Failure modes

| Symptom | Likely cause | Recovery |
|---------|--------------|----------|
| Branch skipped | Re-queued same day | Wait until next calendar day or accept skip |
| 401 on fetch | Bad Variable secrets / expired client | Fix `sam_*` Variables; clear fetch task |
| Empty staging | API returned 0 completed activities | Check date window / category filter |
| dbt skipped | Empty `sam_leads_dbt_job_id` | Set Variable to enable |
| Odoo stub logs | Adapter not wired to pattern 02 | Import production lead engine |
| Slack FAILED | Count mismatch or upstream fail | Diff warehouse lead model vs Odoo creates |
