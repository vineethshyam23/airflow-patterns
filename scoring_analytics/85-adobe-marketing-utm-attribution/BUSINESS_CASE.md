# Business case: Adobe marketing UTM attribution

Marketing needed visit-grain attribution for the product website —
UTM source/medium/campaign, referral channel, and a short conversion
funnel (pricing page → request demo → form submit) — without asking
analysts to re-derive Adobe hit rules in every Looker dashboard.

I kept this as a thin daily append on top of the hourly Adobe
datafeed land (patterns 49 / 51) rather than folding it into the
rawfeed DAG. Land and attribution have different owners, schedules,
and failure modes: if marketing changes funnel definitions at 06:00,
we should not block the hourly unpack that feeds half the warehouse.

## What this unlocked

- One refined table marketing can aggregate without touching hit TSV
- Funnel flags as nullable visit_id markers — easy to `COUNT(DISTINCT)`
  without inventing a second event fact table
- Referral-type pivots (search / social / direct / external / internal)
  aligned with Adobe `referrer_type_id`, not scraped UTM guesses
- UTM eVars (`var_interaction_marketing_*`) carried at hit grain so
  campaign reports can still roll up to visit or visitor

## Constraints

- Schedule is `0 6 * * *`. Upstream hourly datafeed must have today's
  partition before this runs. There is no sensor in production —
  cron hope is real debt; Dataset / ExternalTaskSensor is the fix.
- Load is bare `INSERT`. Same-day re-run duplicates rows. Treat green
  re-runs as unsafe until you add MERGE or delete-then-insert.
- Date filter uses `CURRENT_DATE()`, not `{{ ds }}`. Backfills need a
  manual SQL edit or a Variable override of the date expression.
- `retries: 0`. Transient BigQuery flakes fail the day; match sibling
  Adobe DAGs (`retries: 1`) when you next touch this file.
- Product filter is a single website code. App / other suites stay
  out — intentional scope, not a bug.

## What this is not

Not Adobe Data Feed land (patterns 49 / 51) — those unpack vendor
tar/tsv into staging/trusted/refined hit tables. Not Hydra site-
traffic CSV publish (pattern 77) — that ships already-shaped metric
marts as files. Not the thin dbt Cloud wrappers that replaced some
Adobe report DAGs. This folder is the *marketing attribution*
projection that still ran as Composer SQL for years because the
funnel contract was small and marketing-owned.
