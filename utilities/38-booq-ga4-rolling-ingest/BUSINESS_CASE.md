# Business case: POS vendor GA4 rolling event ingest

Product analytics for one POS vendor estate needs daily GA4 web/app
events in the warehouse — sessions, ecommerce hits, device/geo, and
the nested event_params / items structs Google exports natively.
GA4 already lands daily shards in BigQuery (`events_YYYYMMDD`). The
job is not an API pull; it is a controlled reload of a rolling window
into a staging table our dbt models can trust, plus a Data Transfer
kick for any companion GA tables the BI layer still consumes.

I kept the production shape: seven parallel DELETE+INSERT day loads
(yesterday through day-7), then BigQuery Data Transfer, then one dbt
Cloud job with run-id capture. The 7-day lookback exists because GA4
export shards arrive late and Google sometimes rewrites recent days.
Full history reload is unnecessary; a rolling window is enough and
cheap.

Distinct from pattern 35 (same vendor, but HMAC CSV store-details
master data) and from Adobe rawfeed pipelines. Sibling DAG for a
second POS brand uses the same GA4 pattern without the Data Transfer
step — ship that separately if the engineering delta is still useful.

## What this unlocked

- One 08:00 UTC cron keeps a 7-day staging window fresh without
  backfill scripts
- Per-day DELETE before INSERT makes the load idempotent for that
  date; re-runs do not duplicate
- dbt stays behind the transfer barrier so trusted models only see
  post-transfer state
- Run ids land in an Airflow Variable for ops follow-up when dbt
  Cloud UI is noisy

## Constraints

- Dates use `CURRENT_DATE()` inside the SQL, not `{{ ds }}`. Fine for
  the morning schedule; awkward for deterministic backfills.
- `dagrun_timeout=20 minutes` is tight against 7 BQ jobs + ~6 min
  sleep + dbt timeout of 1000s. Production lived with it; raise the
  budget or shrink the sleep before you add more work.
- Transfer step sleeps a fixed interval and does not poll run state.
  A slow or failed transfer can look green until dbt or BI complains.
- No pre-check that `events_{YYYYMMDD}` exists — missing shards yield
  empty inserts for that day.

## What this is not

Not the vendor establishment CSV (pattern 35). Not Adobe Analytics
rawfeed. Not the dbt models that build product funnels — those live
in the dbt Cloud job referenced by Variable.
