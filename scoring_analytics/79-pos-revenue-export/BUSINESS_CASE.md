# Business case: Platform POS revenue potential export

Wholesale planners and partner scoring already had article
recommendations (pattern 53) and predicted ingredient demand
(pattern 78). What they still lacked was a clean *customer-grain*
revenue potential: trailing-12-month POS net sales, annualised the
same way the SAM `annual_sale` API does it, keyed to wholesale
customer number and home store.

I kept this as its own monthly DAG rather than bolting it onto the
Advanced Replenishment export. Different subject, different SQL
contract (licence lifecycle + week-coverage gate + QUALIFY
uniqueness), and a schedule that has to sit after the
`pos_potential` dbt job — not beside a daily hash-partitioned
quantity feed.

## What this unlocked

- One Avro subject (`platform_pos_revenue`) aligned with SAM
  `annual_sale` so partner and warehouse numbers reconcile
- Annualisation only when the POS licence has been live under a
  year and is not suspended — avoids inflating mature sites
- A four-week coverage gate so thin POS histories do not enter the
  potential population
- `QUALIFY … = 1` on (country, customer_no, home_store_id) so
  ambiguous matches fail closed instead of double-posting
- Sequential DE → FR → ES → IT with `max_active_tasks=1` so one
  OAuth token and one ingest path stay operable under monthly load

## Constraints

- Schedule is `30 6 1 * *`. Upstream `pos_potential` / SAM dbt must
  finish at or before 06:00 on the 1st. Prefer an ExternalTaskSensor
  or Dataset dependency over trusting cron order forever.
- Full monthly load, no watermark. Re-runs re-post the same
  snapshot — coordinate with the partner before a historical replay.
- `end` uses `ALL_DONE`. A failed country does not block later ones
  from being marked done at the DAG level — good for partial
  delivery, bad if "green DAG" is treated as full four-market
  coverage.
- Match join is LEFT then filtered. Establishments without a
  wholesale match never ship; that is intentional until matching
  coverage improves.

## What this is not

Not Advanced Replenishment predicted quantity (pattern 78). Not
ranked menu-gap opportunities (patterns 12/14). Not POS Intelligence
*article* recommendations (pattern 53). Not the SAM API itself —
only the warehouse extract that mirrors its annual_sale contract
onto the event bus.
