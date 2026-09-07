# Business case: Payment wallet API ingest (KYC + transactions + VOP)

Finance and product analytics need yesterday's payment-wallet activity
in BigQuery every morning — KYC onboarding status, authorization /
settlement transactions, and Verification-of-Pay (VOP) performance by
country. The product team exposes those as three OAuth DWH endpoints
with different pagination contracts. The job is to land them into
staging so dbt can build trusted models without the warehouse talking
to the wallet API directly.

I kept extract and transform in one DAG. The API window is fragile
(same-day lag, empty pages that return HTTP 400, count vs no-count
pagination). Coupling the land step to the dbt Cloud jobs means ops
see one green/red signal per morning instead of hunting across two
schedulers when KYC landed but transactions did not.

## What this unlocked

- One 04:10 UTC cron replaces ad-hoc wallet portal exports
- Three feeds share OAuth + Composer → rawzone → staging plumbing
- Empty NDJSON branches skip load without failing the whole DAG
- Transactions use a count endpoint so page math stops at known total;
  KYC walks until an empty page; VOP is one call per `reportDate`
- Per-feed Slack summaries with API vs loaded counts for the
  high-volume transactions path

## Constraints

- Parse-time date window (`datetime.now() - 1 day`). Fine for a stable
  morning schedule; broken for backfills. Prefer `{{ ds }}` /
  `data_interval` if you rewrite.
- Production historically used a day-2 lag when the API trailed; the
  sample defaults to yesterday — tune to the SLA you actually get.
- VOP only exists from a product-side earliest date forward. Multi-day
  catch-up is an optional Variable pair, not catchup=True.
- Staging loads NDJSON as a single JSON column via CSV/TSV loader
  quirks (`field_delimiter="\t"`). It works; a native newline JSON
  load is cleaner if your Composer version supports it.
- KYC staging is TRUNCATE; transactions and VOP are APPEND. dbt owns
  dedupe / SCD for the trusted layer.

## What this is not

Not the outbound KYC Avro export to a partner event bus (pattern 11).
Not Adyen terminal inventory (pattern 03). Not the dbt models
themselves — only the Cloud job triggers after staging lands.
