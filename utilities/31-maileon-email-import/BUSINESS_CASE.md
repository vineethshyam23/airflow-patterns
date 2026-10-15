# Business case: Maileon email marketing import

Email engagement is a first-class input for CRM scoring and
campaign ops. Opens, clicks, bounces, blocks, and unsubscribes
need to land in the warehouse on a predictable daily cadence so
attribution and suppression lists stay honest.

## Problem

Maileon exposes engagement as XML report endpoints with paging.
Marketing wants eight grains in BigQuery by morning, plus human-
readable mailing names/tags so analysts are not joining on opaque
IDs. Empty campaign days are normal — a hard failure on zero-byte
extracts would page people for nothing.

## Approach

Composer owns the graph. A shared client pages each report, converts
XML to JSONL on the Composer data volume, then a BranchPythonOperator
checks blob size before the rawzone copy. Staging tables are
WRITE_TRUNCATE per report. After all loads, dbt builds intermediate
models; a second wave looks up mailing name and tags per ID (with
429/5xx backoff) and two more dbt jobs fold that metadata in.

I kept the empty-file branch because it matches how this ran in
production. The join back onto the load task with
`none_failed_or_skipped` is the tradeoff worth calling out: a skip
path can still TRUNCATE staging when no object was copied. If I were
rebuilding this, the skip path would end the branch without loading.

## Why not a single wide extract?

Report schemas differ (link fields on clicks, status transitions on
blocks). Parallel branches keep failures isolated — one flaky
endpoint does not block the other seven — and schema JSON stays
small and reviewable.

## Constraints

- API key via Airflow Variable (Basic auth); no key material in code
- Recipients report carries email addresses — treat as PII in
  access controls and retention
- Name/tag enrichment is O(mailing_ids) sequential calls; plan for
  rate limits and long task duration
- dbt job IDs externalized; transform job is short, metadata jobs
  were given multi-hour timeouts in production

## Out of scope here

Cloud Function / Cloud Run Maileon trigger APIs belong with the
API Integrations portfolio, not this DAG pattern.
