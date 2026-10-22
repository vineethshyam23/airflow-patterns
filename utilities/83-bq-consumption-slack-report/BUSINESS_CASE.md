# Business case: BigQuery consumption Slack digest

Warehouse spend is dominated by a handful of humans writing wide
scans. Waiting for the monthly invoice is too late; by then the
burn is already in the books. I wanted a daily social signal in the
ops channel — not a dashboard that nobody opens before standup.

This DAG reads a pre-aggregated `monitoring.bq_usage` table (built
elsewhere from INFORMATION_SCHEMA / billing export), ranks yesterday's
top human consumers, calls out the heaviest user on record, and posts
month-to-date GiB + EUR. Service accounts are filtered out so the
leaderboard stays about people, not Composer itself.

## What this unlocked

- A 06:30 UTC habit: the channel sees who scanned what before the
  first coffee meeting
- GB/query ratio as a blunt efficiency proxy — high ratio usually
  means missing partition filters or `SELECT *` over wide facts
- Soft social pressure without paging: medals and descriptors are
  loud enough that habitual offenders get asked to fix the SQL
- Month-to-date totals that make budget conversations concrete mid-month

## Constraints

- Depends on `monitoring.bq_usage` being populated for yesterday.
  Empty yesterday → sparse Slack header, not a hard failure.
- `retries: 0`. Transient BQ or Slack blips fail the run; that was
  accepted because the report is informational, not a gate.
- SlackWebhookOperator runs inside the Python callable. Airflow does
  not track the Slack step as its own task — fine for a one-task
  digest, awkward if you want separate retry policy on delivery.
- The "30 day record" line is a production label mismatch: the SQL
  has no 30-day window. Fix the label or add the filter before you
  trust it in a FinOps review.

## What this is not

Not pattern 80 (Slack on parallel materialization failures). Not a
billing-export land or INFORMATION_SCHEMA scraper — those live
upstream of `monitoring.bq_usage`. Not GCP FinOps portfolio write-up;
this is the Composer-side daily ops digest only.
