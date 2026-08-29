# Business case: AppFigures weekly mobile analytics ingest

Product and growth need a reliable weekly picture of how the mobile
apps are doing — downloads / sales by country, star ratings over time,
and rating rollups by product and country. AppFigures already has that
data behind a REST API. The warehouse job is to land last week's slice
in a shape analytics and dbt can trust, without someone downloading
CSVs by hand every Monday.

I kept the Composer DAG as four identical chains (one per report
shape) fan-in to a stage barrier, then one dbt Cloud job. Staging is
truncate; trusted is append. That matches how the rest of the
platform treated vendor CSV landings at the time: cheap to reason
about, painful on re-runs if you forget to prune the week first.

## What this unlocked

- One Monday cron covers four report grains without four calendars
- Raw CSVs under `appfigures/{report}/{end_date}/` for audit / re-load
- Staging isolate so a bad schema load does not corrupt trusted mid-week
- Downstream dbt models only fire after all four chains succeed

## Constraints

- Trusted is append-only. A re-run of the same week duplicates rows
  unless you delete that week first. Acceptable for a weekly batch
  with low volume; would not copy this for daily high-churn facts.
- Week window was computed at DAG parse time in production. Fine for
  a stable Monday schedule; broken for backfills and long scheduler
  restarts. Documented; not silently "fixed" in the sample.
- API auth must live in Variable / Secret Manager. The production
  file had a hardcoded Bearer PAT — that is the first thing I would
  change in any rewrite.
- No HTTP status check in the original fetch helper. Empty or error
  bodies could land as CSVs. The sanitized helper fails closed.

## What this is not

Not the older basic-auth AppFigures script that dumped JSON products /
reviews into a raw bucket. Not the dbt models that turn trusted into
dashboards. Not a real-time ratings webhook — weekly batch only.
