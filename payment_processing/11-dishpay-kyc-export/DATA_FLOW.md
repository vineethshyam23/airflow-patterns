# Data flow: Payment KYC export to partner event bus

Schedule: `0 6 * * *` (daily 06:00 UTC). Catchup off — a missed day
is an explicit re-trigger, not a backfill storm of additive posts.

## Stage A — dbt refresh

`dbt_payment_kyc_refresh` runs the Cloud job:

1. `stg_payment_kyc` — filter raw KYC to the pilot country + payment
   product code
2. `payment_kyc_snapshot` — SCD Type 2 historisation
3. `int_payment_kyc` — reshape SCD2 columns for export
4. `payment_kyc_export` — current valid rows (`_valid_flag = true`)

If dbt fails, ingest stays blocked. That is correct: posting stale
valid rows as today's KYC status is worse than a delayed partner feed.

## Stage B — Avro ingest

| Task | Source | Sink |
|------|--------|------|
| `export_payment_kyc_PL` | `refined.payment_kyc_export` (full table) | `POST /ingestbulk/pl/{schema_id}` |

Pilot country when this shipped: `pl`. The query module exposes
`PaymentKyc.countries` for fan-out; do not add markets until the
schema id and consumer are registered.

Each task: SELECT → Avro encode → POST chunks of 500.

Exported fields: establishment_id, country_code, kyc_created_dt,
kyc_modified_dt, kyc_step, kyc_step_details, kyc_status,
kyc_duration_day, kyc_duration_month, kyc_onboarding_successful,
kyc_first_attempt, kyc_adyen_pending_validation,
kyc_adyen_error_validation.

## Idempotency and re-runs

- Re-run after dbt success: re-posts the full country snapshot. Safe
  if the bus upserts on natural keys; coordinate otherwise.
- Re-run dbt alone: refreshes refined; ingest must follow or the bus
  still holds yesterday's payload.
- Never put ingest before dbt. You will ship yesterday's valid rows.

## Failure modes worth knowing

- OAuth 401 mid-chunk: client clears token and retries the POST once
  with the same body (production dropped the body on retry — fixed
  here).
- Empty refined table: often a dbt filter / product-code change, not
  a broken OAuth path — check staging row counts before paging the
  event API.
- dbt timeout (600s): usually snapshot contention, not KYC volume.
  Bumping timeout without checking the Cloud job graph hides the real
  issue.
- HTTP 4xx/5xx on ingest: sanitized path raises (`raise_for_status`).
  Production logged the JSON and continued — that left silent gaps.
- No post-ingest row-count check in this pattern. If you need proof
  the bus accepted every chunk, add a validation task; do not assume
  log lines equal delivery.
