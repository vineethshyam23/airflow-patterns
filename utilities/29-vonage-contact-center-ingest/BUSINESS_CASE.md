# Business case: Vonage Contact Center daily stats ingest

Sales and service ops needed yesterday's contact-center picture —
who was available, how long interactions sat in queue, what agents
actually did — in the warehouse next to CRM and ticket data. Vonage
exposes that behind a stats API with OAuth2 client-credentials and
paged JSON. The Composer job's job is to land five grains every
morning so dbt can shape them for dashboards, without someone
exporting CSVs from the vendor UI.

I kept five parallel chains that fan into a stage barrier, then one
dbt Cloud job, then a soft Slack status that compares API
`totalCount` to refined rows loaded today. Staging stores each
NDJSON line as a single BigQuery JSON column. That looked odd the
first time I saw it; it is deliberate. Contact-center payloads
change field names more often than we want to chase in the load
step. Opaque JSON + dbt unpack keeps the extract stable.

## What this unlocked

- One 04:10 cron covers five stats grains without five calendars
- Raw NDJSON under `vonage/{grain}/{load_date}/` for audit / re-load
- Staging isolate so a bad vendor day does not touch refined mid-run
- Downstream dbt only fires after all five staging loads finish
- Ops can see API vs refined counts in Slack without opening BQ

## Constraints

- Window is calendar yesterday at wall-clock time. Fine for a stable
  daily schedule; awkward for backfills and long scheduler restarts.
  Prefer data-interval macros in a rewrite.
- Production left a `day_before_yesterday` TODO about vendor lag but
  never wired it into the live window. I kept yesterday and called
  that out rather than inventing a lag policy.
- Staging is truncate per grain per day. Re-runs of the same day
  replace staging; refined behaviour depends on the dbt models
  (usually merge / incremental on `loaded_date`).
- Agent status is a point-in-time snapshot with no start/end filter.
  The other four grains take the yesterday window.
- OAuth secrets must live in Variable / Secret Manager. The original
  helper had a working client pair in `__main__` — first thing to
  delete in any rewrite.
- Slack status uses `ALL_DONE` so a failed grain still gets a
  failure note. Prefer `ALL_SUCCESS` on the dbt barrier if you
  rewrite and want stricter gating.

## What this is not

Not the archived fiscal-year histload that chunked months to dodge
501s. Not Freshdesk or AppFigures. Not the dbt models that unpack
`value` into refined sales tables. Daily batch only.
