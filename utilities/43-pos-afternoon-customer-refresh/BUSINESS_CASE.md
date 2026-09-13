# Business case: Midday POS customer-master refresh

## Problem

The overnight POS vendor ingest lands the full dump set (transactions,
products, establishments, debtors, locations). Sales and matching
models that depend on *customer master* alone still looked stale by
early afternoon whenever the vendor pushed a same-day debtor or
location correction after the overnight window.

Re-running the whole overnight DAG at midday was the wrong fix: too
much compute, too much contention on shared dbt jobs, and no need to
touch transaction grains that had not moved.

## Decision

A second, narrow Composer DAG owns the afternoon path:

1. **Timezone-aware 13:00 Europe/Amsterdam schedule** — `CronTriggerTimetable`
   so the midday refresh stays at 1pm local through CET/CEST flips.
2. **Two tables only** — debtor + location (debloc). Everything else
   waits for overnight.
3. **Latest same-day blob** — list by vendor prefix, filter on today's
   date token in the object name, pick `time_created` max. Mid-morning
   re-drops replace the earlier file without a new DAG.
4. **Staging TRUNCATE → shared dbt chain** — customer model, matching
   IDs, POS models, Tableau pack; then materialize the refined
   customer-base view as a physical table for consumers that cannot
   query views under their BI extract pattern.

This is not pattern 35 (HMAC store-details API) and not pattern 38
(GA4 events). Same drop zone as overnight, different schedule and
table scope.

## Constraints I cared about

- DST drift on a fixed UTC cron would put the refresh at noon or 2pm
  for half the year; local timetable was non-negotiable for ops.
- Missing same-day file must soft-skip that table, not fail the DAG —
  vendor sometimes lands location later than debtor.
- `max_active_runs=1` so a slow dbt chain cannot overlap the next day.
- Hash/SCD merge stays in the overnight / dbt path; afternoon only
  re-lands staging. Do not duplicate SCD SQL here.
- Parse-time date token matches production behaviour (and its backfill
  limitation). Document it; do not silently switch to `{{ ds }}` in the
  sample without updating the object-name contract.

## Outcome

Customer master and matching IDs are current for afternoon reporting
without paying for a second full POS run. Ops has one Slack failure
path and a clear split: overnight = full set, afternoon = debtor +
location only.
