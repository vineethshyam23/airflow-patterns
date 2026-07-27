# Business case: weekly active asset ID snapshot

Asset lifecycle exports (pattern 09) tell the partner what changed —
status flips, new subscriptions, voucher codes. They do not tell the
partner what disappeared. Odoo hard-deletes and weekly cleanup remove
sale_order_line rows from refined sales; without a positive "still
active" signal, the partner master-file keeps stale assets forever.

This DAG ships a weekly full snapshot of active sale_order_line IDs
across 13 markets. The consumer LEFT JOINs lifecycle rows on
`sale_order_line_id`. Anything missing from the latest snapshot is
treated as deleted. Four columns, no SCD, no dbt — just the ID set
after cleanup has already run.

## What this unlocked

- Reliable deletion detection without asking Odoo for soft-delete
  flags that do not exist on every line
- One weekly contract for 13 countries instead of 13 ad-hoc extracts
- Clear separation from pattern 09: deltas for status, snapshot for
  presence

I kept this as its own Composer DAG. Folding it into the twice-daily
lifecycle DAG would couple a weekly full-table export to a same-day
SCD delta schedule and make partial-country failures harder to reason
about on Sunday mornings.

## Constraints

- Full snapshot every run, not incremental. Re-posts the entire active
  set. Coordinate with the consumer before a historical replay —
  ingest is additive on the bus side.
- Depends on upstream weekly cleanup having finished. Without that,
  deleted lines still look active. Source did not wire an
  ExternalTaskSensor; production ops coordinated by schedule. Adding
  a sensor is the obvious next hardening step.
- `end` uses `ALL_DONE` so one country failure does not block the DAG
  from closing. Monitor task-level failures; do not trust DAG success
  alone for "all 13 markets shipped."
- Result set buffered in memory before chunking (500). Fine per
  country at current volumes; revisit if a market balloons.

## What this is not

Not the twice-daily lead/asset/voucher lifecycle fan-out (pattern 09).
Not SFDC asset history (pattern 05). Not an Odoo Postgres pull. This
DAG stops at "refined sales reflects current active lines and the
partner bus has this week's ID set for every registered market."
