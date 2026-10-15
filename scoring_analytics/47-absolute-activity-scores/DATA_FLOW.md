# Data flow: absolute multi-channel activity scores

## Path A — Base rebuild (start of month)

1. `as_absolute_base_wb_rt_event` scans derived events for CMS /
   reservation establishment IDs, renames noisy event labels, drops
   excluded regions and high-frequency spike days, WRITE_TRUNCATE into
   a day-partitioned base.
2. `as_absolute_base_rt_reservation` aggregates reservations by CRM id
   and month.
3. `as_absolute_base_adobe` unions product logins with Adobe visit /
   visitor facts (CMS, reservation, web listing, order tool).
4. `as_absolute_base_adobe_mk` builds the menu-kit login base separately
   (different product_type filter and grain).

Failure mode: if Adobe lag is late on the 1st, Adobe-dependent channels
block; reservation/event bases can still complete. Do not mark the DAG
success until fan-in finishes.

## Path B — Per-channel append

Each channel task reads its base (and customer-base / CRM facts where
needed), computes rolling activity metrics for
`as_month = next_execution_date month-floor`, and WRITE_APPENDs a
month-partitioned refined table.

Channels:

- Website: login/visitor + event
- Reservation tool: login + event + reservation
- Web listing login
- Menu kit login
- Order tool: login/event + orders (test-order comment filter)
- CRM inbound/outbound call aggregates
- Portal + mobile app login aggregates

Failure mode: APPEND without a prior DELETE for the month will duplicate
rows on retry. Production schedule discipline avoided this; a rebuild
should DELETE `WHERE as_month = …` first.

## Path C — Fan-in score

`as_absolute_activity_score` FULL OUTER JOINs all channel tables for the
target month, joins `external.as_absolute_kpi_thresholds` and
`customer_base_establishment`, derives boolean flags, and APPENDs the
0–3 score row (partitioned on `as_month`).

Depends on every channel task. That barrier is intentional.

## Path D — MoM transitions

`ce_activity_score_transitions` compares current vs prior month scores
and WRITE_TRUNCATEs boolean transition flags into `trusted`. Downstream
engagement reports read this table only — they never re-derive MoM math.

## Partition / schedule contract

| Concern | Choice |
|---------|--------|
| Schedule | `15 7 1 * *` (1st of month 07:15 UTC) |
| Month key | `next_execution_date` floored to day 1 |
| Channel / score write | WRITE_APPEND + DAY partition on `as_month` |
| Bases / MoM | WRITE_TRUNCATE |
| Concurrency | `max_active_runs=1` |

## Downstream consumers

- Customer engagement scorecards (MoM up/down)
- CRM / success workflows filtering `establishment_active`
- Later: dbt Cloud job that reimplemented Paths B–D with tests
  (`etl_activity_score_job`) — same grain, better lineage
