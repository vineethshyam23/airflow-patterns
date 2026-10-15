# Data flow: Vonage Contact Center daily ingest

## Run order

1. Scheduler fires ~04:10 UTC (`10 4 * * *`).
2. Window = calendar yesterday 00:00:00 → 23:59:59 (wall clock).
3. For each grain in parallel:
   - `data_fetch_*` OAuth + paginated GET →
     `data/vonage/{name}.ndjson` on Composer
   - `upload_storage_*` copies to
     `gs://{rawzone}/vonage/{name}/{load_date}/{name}.ndjson`
   - `load_staging_*` TRUNCATEs
     `{project}.trusted_staging.{name}` as JSON `value`
4. `stage` waits for all five chains (`TriggerRule.ALL_DONE`).
5. `vonage_dbt` runs the Cloud job (or EmptyOperator in sample).
6. `stage_1` → `check_all_tasks` → per-grain refined count →
   `stage_2` → Slack → `end`.

## Grains

| file_name | API path | Date filter |
|-----------|----------|-------------|
| vonage_agent_activities | /stats/agent-activities | yesterday start/end |
| vonage_agent_presence | /stats/agent-activities/presence | yesterday start/end |
| vonage_agent_status | /stats/agent-status | none (snapshot) |
| vonage_interactions | /stats/interactions | yesterday start/end |
| vonage_queue_times | /stats/queue-times | yesterday start/end |

Non-status calls also pass `include=Processed`.

## Date window

```python
yesterday = datetime.now() - timedelta(days=1)
start = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
end   = datetime(y, m, d, 23, 59, 59)
```

Production evaluated this at **module import** (same habit as
AppFigures). That means:

- Steady daily runs usually get the intended prior day
- Backfills / long-lived DAG processors can pin a stale window
- Prefer `{{ data_interval_start }}` macros in a rewrite

A `day_before_yesterday` variable existed with a vendor-lag TODO and
was never used in the live window. Sample keeps yesterday only.

## Pagination

Default `limit=500`. Loop increments `page` until the page is empty
or `len(all_items) >= meta.totalCount`. On 401/403, refresh the
token once and retry the same page.

## Idempotency

- Staging: truncate per grain per run — safe to re-load the same day
- Raw GCS: same object path for the day — overwrite on re-run
- Refined: owned by dbt (typically incremental on `loaded_date`)
- Recovery: clear from fetch or upload and re-run the chain; prune
  refined for that `loaded_date` if dbt is not idempotent

## Failure modes

| Failure | Effect | What to do |
|---------|--------|------------|
| Missing / bad `vonage_creds` | Token raise; chain fails | Fix Variable; re-run |
| API 5xx / timeout | Task retries (3 × 10 min) | Wait vendor; then clear |
| Mid-page 401 after refresh fails | Fetch raises | Rotate client secret |
| One chain fails, others OK | Stage may still fire (`ALL_DONE`) | Prefer `ALL_SUCCESS` on rewrite |
| dbt timeout (300s) | Job fails after loads | Raise timeout or slim models |
| Slack webhook missing | Logged; DAG continues | Fix conn Variable |
| API count != refined count | Slack success still fires | Check dbt filters / lag |

## Scale notes

Daily contact-center stats are moderate. Wall-clock is API
pagination + five BQ loads + dbt, not compute. Keep
`max_active_runs=1` so overlapping mornings cannot double-fire Slack
or stomp staging mid-load. For multi-month backfills, use the
archived chunked histload pattern (3 chunks per month) — not this
DAG as-is; large windows hit vendor 501s.
