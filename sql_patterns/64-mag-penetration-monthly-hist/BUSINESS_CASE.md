# Business Case: MAG penetration monthly historization

Group reporting needs a frozen month-end view of wholesale and platform
penetration — not a live view that drifts as customer-base flags flip
mid-month. The question is simple: for last month, how many active /
paying establishments did we have per country and at corporate rollup,
across product segments (suite, brick, vendor POS combinations)?

I kept this as a small Composer DAG on the **2nd of the month** rather
than baking historization into the daily refined spine. Daily zone jobs
already rebuild live metrics; historization is a calendar contract.
Finance closes the prior month on day 1 (sales / acquisitions sibling);
day 2 locks penetration. Splitting those calendars meant a failed sales
hist did not block penetration, and vice versa.

The corporate row is the interesting engineering choice. Country rows
are straight sums from the live penetration view. The ``corp`` row is
**not** a re-aggregation of those countries — it is a delta against the
prior month's hist, computed from the customer-base establishment grain.
That matches how group reporting historically defined corporate
movement. It also means this DAG is not a pure append of a SELECT; task
2 reads the table task 1 just wrote.

## What this unlocks

- Append-only month grain on
  `refined.hist_penetration_rates_reporting` for ~20 segment metrics.
- Country actuals and corporate delta in one sequential chain so the
  corp subquery sees the month's country rows when it needs them.
- A stable upstream for pattern 24 (partner penetration export) and for
  VCD / MAG consumers that read hist tables.

## Tradeoffs I accepted

- `WRITE_APPEND` with no delete-before-insert. Re-running the DAG for
  the same month duplicates rows. Cheap to operate; expensive to
  explain when someone clears a failed task and hits "clear downstream".
- `TriggerRule.ALL_DONE` on the corp task (production behaviour). If
  country append fails, corp still runs and can write a nonsense delta.
  I leave that visible rather than silently "fixing" it in the
  portfolio — interviewers should see the risk.
- Inline SQL builders instead of dbt models. These queries are
  calendar-bound and rarely change; moving them to dbt would not remove
  the Airflow schedule or the sequential dependency.

## Not this pattern

- Pattern 24: monthly *export* of penetration / acquisition hist to the
  partner event bus. This pattern *writes* the hist table 24 reads.
- Pattern 46 / 48 / 62: refined / product / VCD zone rebuilds
  (CREATE OR REPLACE or fan-out barriers), not month-grain append.
- Archived `etl_refined_zone_monthly` (1st-of-month sales +
  acquisitions hist + Metro ID clean) — sibling calendar, different
  tables. Candidate for a later ship if depth warrants.
