# Data flow: POS Intelligence recommendations export

## Run order

1. Upstream Vertex / foodgraph job materializes
   `ml_project.{foodgraph_preprocessed*}.pos_article_final_recommendation_{CC}`
   (not in this folder).
2. On the 1st at 06:15 UTC, Composer starts
   `etl_pos_intelligence_recommendations_export`.
3. For each ISO in `COUNTRY_ISO_CODES`, in order:
   - `export_pos_intelligence_{CC}` (4h timeout)
4. Final `end` (`ALL_DONE`).

## Per-country path

```
BQ SELECT (full recommendation table, typed casts)
  → row iterator
  → Avro binary encode (schema parsed once)
  → base64 value records
  → POST chunks of 500 to /ingestbulk/{cc}/{schema_id}
```

Dates ship as `YYYY-MM-DD` strings. Numeric / bool fields keep Avro
long / double / boolean — do not string-cast everything or the
partner schema registry rejects the payload.

`menu_item_names` is never selected. Including it breaks schema v1
even if the Vertex table has the column.

## Idempotency

- Full-load SELECT has no watermark. Every monthly run reships the
  current recommendation snapshot.
- The sink is append-oriented. Duplicate posts are a consumer
  concern; do not treat "task success" as "exactly-once at the
  partner".
- Re-running a failed country re-ships that market only — later
  countries (once added) are independent tasks.

## Failure modes

| Failure | Effect | What to do |
|---------|--------|------------|
| OAuth all grants exhausted | Task fails at startup or mid-chunk | Check Variable secrets / realm / grant type |
| Transient HTTP | Linear backoff up to 10 attempts | Usually self-heals; else check body size |
| 401 mid-load | Token refresh + same chunk retry | If loops, secret rotation race — pause and fix Variables |
| Vertex table missing for CC | BQ query fails | Do not append ISO until `_{CC}` exists |
| SOURCE_DATASET still on dev in prod | Ships stale / wrong environment rows | Flip to ACC/prod preprocessed when ready |
| 413 / payload too large | Chunk POST fails | Lower `CHUNK_SIZE` |

## Scale notes

Pilot market is FR only. Chunk size 500 and a single active task keep
worker memory flat; wall-clock is dominated by HTTP, not the BQ
scan. When DE (or others) join, keep sequential countries first —
add hash partitions only if one market's full stream exceeds the
4-hour timeout.
