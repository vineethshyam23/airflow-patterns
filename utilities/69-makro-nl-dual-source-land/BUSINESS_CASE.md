# Business case: Wholesale NL dual-source land (MCC API + CHD CSV)

One wholesale market (NL) needs two daily feeds into the same enrichment
lane: the partner's own customer base (mutations since yesterday via MCC
API) and a competitive hospitality market file dropped as CSV into a
landing bucket. Downstream CRM export and discovery joins expect both
staging tables refreshed before dbt builds the enrichment models.

I kept the three chains independent under one DAG. Customer API and
merge-request extract can succeed or fail without blocking the CHD
branch, and CHD ShortCircuits cleanly when the vendor did not drop a
file that day. That matches how the ops calendar actually works —
mutation API is daily; market CSVs arrive irregularly.

## What this unlocked

- One 07:00 UTC schedule covers API customer land + optional market CSV
- Mutation-window API pull (`last_mutday_from={{ ds }}`) avoids full
  reloads of the partner customer estate
- Dirty integer fields (whitespace, text in PHONE) are cleaned in GCS
  before a `max_bad_records=0` BigQuery load — fail loud after clean,
  not after a half-loaded table
- Opaque partner JSON lands as a single JSON column so schema drift in
  the customer payload does not break the load step; dbt owns unnest

## Constraints

- Production originally set `dagrun_timeout=20m` while dbt tasks allow
  60m. Slow dbt runs timed the DAG out before the job finished. The
  sanitized graph raises DAG timeout to 60m; keep job ids in Variables.
- Customer load uses `trigger_rule=all_done` so a merge-request failure
  does not strand the customer → dbt chain. That is intentional
  isolation, not a bug — but it means "green customer branch" can hide
  a red merge branch unless you watch both.
- CHD archive moves files to `processed/` after load. Replay means
  moving objects back; there is no watermark table.
- Credentials live in Airflow Variables (OAuth client + password grant).
  Never hardcode; the production module once had commented secrets —
  stripped here.

## What this is not

Not the HubSpot / MCC reverse export (sibling DAG
`etl_makro_hubspot_export` — next candidate). Not SEO NDJSON land
(pattern 25) or POS HMAC CSV land (pattern 35). Not the dbt models
that build discovery enrichment tables.
