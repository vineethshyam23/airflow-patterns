# Data flow: BQ API refined zone + AlloyDB sync

## Schedule

- Cron: `2 6 * * *` (06:02 UTC)
- `max_active_runs=1`, `catchup=False`
- Retries: 2 / 10-minute delay
- AlloyDB task `execution_timeout`: 2 hours (sample addition; source had none)

## Run order

1. **start**
2. **Parallel BQ WRITE_TRUNCATE** (7 tasks)
   - `api_product_website_refined` → `refined.api_product_web`
   - `api_product_reservation_refined` → `refined.api_product_reservations`
   - `api_product_order_refined` → `refined.api_product_order`
   - `api_product_establishment_refined` → `refined.api_product_establishment`
   - `api_product_pos_refined` → `refined.api_product_pos`
   - `api_dashboard_market_refined` → `refined.api_dashboard_market`
   - `api_co_map` → `trusted_staging.api_co` (wholesale_id → establishment)
3. **load_data_to_alloydb** (after all seven succeed)
   - `SELECT max(date(created_date))` from AlloyDB (bootstrap `2024-06-01` if empty)
   - `SELECT DISTINCT *` from BQ where `created_date` between max and `current_date()`
   - Row insert with `ON CONFLICT (unique_key) DO NOTHING`
   - Log processed / inserted / skipped

## AlloyDB columns

`unique_key`, `subject`, `assigned_id`, `description`, `type`,
`created_date`, `sub_type`, `event_topic`, `is_closed`, `name_id`,
`outcome`, `activity_id`, `start_date`, `lead_account_id`,
`wholesale_account_identifier`, `user_id`, `email`, `profile_name`,
`won_status`

## Idempotency

| Layer | Behaviour on re-run |
|-------|---------------------|
| BQ `api_*` | Full truncate-reload — always current snapshot |
| AlloyDB | Conflict on `unique_key` skips duplicates; boundary day re-scanned |

## Failure modes

- **One BQ task fails** — fan-in blocks AlloyDB sync; other tables may
  already be truncated to the new query. Fix SQL / upstream, re-run DAG.
- **AlloyDB network / auth** — Variables JSON shape must match
  `psycopg2.connect` kwargs (`host`, `port`, `dbname`, `user`, `password`).
- **Row insert exception** — fails the whole sync task; prior inserts in
  that attempt already committed (`autocommit=True`). Re-run is safe
  thanks to `ON CONFLICT`.
- **DEV conn mismatch** — BQ tasks use `bigquery_default` regardless of
  `gcp_conn_id`. Expect wrong-project surprises in DEV until wired.

## Optional upstream (disabled in source)

An `ExternalTaskSensor` on a Salesforce delivery-order DAG was
commented out. Re-enable if establishment maps must wait on that
pipeline before API refresh.
