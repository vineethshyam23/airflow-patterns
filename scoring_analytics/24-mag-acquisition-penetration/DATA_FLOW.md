# Data flow: MAG acquisition + penetration monthly export

## Run order

1. Upstream month-end refine materializes
   `refined.hist_acquisitions_reporting` and
   `refined.hist_penetration_rates_reporting` (not in this folder).
2. On the 2nd at 15:45 UTC, Composer starts
   `etl_mag_reporting_export`.
3. Two independent chains start (no edge between them):
   - `ingest_acquisition_{cc}` for each country in order
   - `ingest_penetration_{cc}` for each country in order
4. Each task uses `ALL_DONE` so a prior failure does not block the
   next market.

## Per-task path

```
BQ SELECT (full history for one country)
  → row iterator
  → Avro binary encode (schema parsed once)
  → base64 value records
  → POST chunks of 500 to /ingestbulk/{cc}/{schema_id}
```

Acquisition schema id ≠ penetration schema id. Both come from Airflow
Variables.

## Country mapping

| Composer `country` | Warehouse filter | Notes |
|--------------------|------------------|-------|
| `hr` … `ua` (ISO)  | `lower(country)` / `lower(reseller_country)` = ISO | Penetration wraps metrics in `IFNULL(..., 0)` |
| `ag`               | `corp`           | Corporate / aggregate rollup; penetration leaves nulls alone |

## Idempotency

- Each run re-selects the full historical table for that country —
  there is no watermark filter.
- The sink is append-oriented. Duplicate posts are a consumer
  concern; do not treat "task success" as "exactly-once at the
  partner".
- Re-running a failed market re-ships the same history slice.

## Failure modes

| Failure | Effect | What to do |
|---------|--------|------------|
| OAuth 401 | Token cleared; same payload retried; then raises | Check Variable secrets / grant type |
| Transient HTTP | Task retries (3 × 10 min) | Usually self-heals |
| One market fails | Later markets still run (`ALL_DONE`) | Clear the failed task; do not assume full coverage from DAG green |
| Upstream refine late | Export ships stale / empty history | Add Dataset sensor or move schedule |
| Wrong aggregate mapping | Empty `ag` payload | Confirm warehouse still uses `corp` for the rollup |

## Scale notes

Payloads are tiny — a few fields per date row, not establishment-
grain dumps. Wall-clock is dominated by sequential HTTP across 17
markets × 2 chains, not by BigQuery. Chunk size 500 matches the
other event-ingest DAGs; leave it alone unless the API starts
413-ing.
