# Data flow: Establishment market-data monthly export

## Run order

1. Upstream foodgraph / SEO refine materializes
   `refined.establishment_market_data_{cc}` (not in this folder).
2. On the 1st at 06:30 UTC, Composer starts
   `etl_establishment_market_data_export`.
3. For each active ISO (from `countries.ACTIVE_ISO_CODES`), in order:
   - `start_{cc}`
   - five parallel `export_{cc}_batch_{0..4}` tasks
   - `end_{cc}` (`ALL_DONE`)
4. Final `end` (`ALL_DONE`).

## Per-batch path

```
BQ SELECT (full country ∩ hash shard)
  → row iterator
  → Avro binary encode (schema parsed once)
  → base64 value records
  → POST chunks of 1000 to /ingestbulk/{cc}/{schema_id}
```

Geo fields cast to FLOAT64 in SQL. Timestamps formatted as strings.
JSON-typed BQ columns are selected raw and stringified in
`_row_to_avro_dict` — `CAST(JSON AS STRING)` is illegal in BigQuery.

## Idempotency

- Hash shards are deterministic for a given `establishment_id` and
  `TOTAL_BATCHES`. Re-running a failed batch re-ships the same slice.
- The sink is append-oriented. Duplicate posts are a consumer concern;
  do not treat "task success" as "exactly-once at the partner".
- There is no watermark / `_update_ts` filter on the export SELECT —
  every monthly run is a full country reship by design.

## Failure modes

| Failure | Effect | What to do |
|---------|--------|------------|
| OAuth 401 storm | Retries refresh token; then raises | Check Variable secrets / grant type |
| Transient HTTP / JSON decode | Linear backoff up to 10 attempts | Usually self-heals; else check body size |
| One batch fails | Later countries still run (`ALL_DONE`) | Clear the failed task; do not assume full coverage from DAG green |
| Upstream refine late | Export ships stale table | Add Dataset sensor or move schedule |
| 413 / payload too large | Chunk POST fails | Lower `CHUNK_SIZE` |

## Scale notes

First production markets were ES and DE — each on the order of a few
hundred thousand establishments. Five shards × 1000-row chunks keeps
worker memory flat; wall-clock is dominated by HTTP, not the BQ scan.
