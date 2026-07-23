# Business case: Odoo / CRM assets + leads lifecycle export

Partner-market reporting for France needed three related feeds on the
same cadence: lead status changes, asset / subscription lifecycle, and
new voucher codes. All three land in refined BigQuery models via one
dbt job; the partner event bus expects three registered Avro schemas.

I kept them in one Composer DAG on purpose. Splitting into three DAGs
with an ExternalTaskSensor on the dbt step looks tidy on a whiteboard
and fails messier in ops — three schedules, three failure emails, and
three people asking whether the refined refresh actually ran. One chain:
refresh once, fan out three ingests.

## What this unlocked

- Same-day FR visibility into lead converts / losses and asset status
  changes (twice-daily schedule at 06:00 and 13:00 UTC)
- SCD Type 2 validity columns as the delta contract — no hist table or
  keyhash compare on the Composer side for leads/assets
- Voucher codes on a created-date window (yesterday+) so a morning miss
  still catches late previous-day creates on the afternoon run

This sits next to pattern 05 (SFDC asset history hash-delta) and pattern
08 (WSL invoices dual sink) but solves a different problem: multi-schema
CRM/Odoo lifecycle fan-out after a shared dbt refresh, scoped to one
partner market.

## Constraints

- Country is hard-coded `fr`. Partner schema registration and field
  semantics are FR-specific in the source deployment. Adding a second
  country is an explicit product decision, not a for-loop.
- Lead/asset delta is `_valid_flag = TRUE AND _valid_from >= today`.
  Miss a day and you miss that day's versions unless you widen the
  window or backfill dbt + re-run.
- Full result set per dataset is loaded into memory before chunking.
  Fine for one country; revisit if this ever goes multi-market.
- Event ingest is additive. Re-runs re-post the same rows — coordinate
  with the consumer before a historical replay.

## What this is not

Not the weekly active sale-order-line ID snapshot (sibling DAG /
module method). Not SFDC Bulk API history export (pattern 05). Not the
Odoo Postgres pull extractor. This DAG stops at "refined lifecycle
models are fresh and today's FR deltas are on the event bus."
