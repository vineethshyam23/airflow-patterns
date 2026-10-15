# Business case: Establishment market-data monthly export

Partner systems needed a country-complete view of establishment
listings — name, address, geo, contact, ratings, hours, delivery
flags, social pages, topics — without warehouse access. Upstream
foodgraph / SEO refine already materializes
`refined.establishment_market_data_{cc}` on the 1st; this DAG ships
it.

I chose a monthly *full* reship over a hash-delta. Listing attributes
span dozens of columns (hours, rating distributions, nested topics).
A whole-row hash would thrash on cosmetic JSON reorderings, and a
column-subset hash would silently drop changes the partner cared
about. Full load costs more bytes; it costs far less on-call time.

## What this unlocked

- Deterministic monthly snapshot per market on the event bus
- Stable parallelization via
  `FARM_FINGERPRINT(establishment_id) MOD N` — no batch column to
  maintain on the refined table
- Streaming BQ → Avro → POST so a ~300k-row country does not OOM the
  worker
- Same OAuth / Avro / chunk pattern as other event-ingest DAGs, so
  ops already know the failure modes

## Constraints

- Schedule is `30 6 1 * *`. Upstream refine must finish earlier that
  morning; if foodgraph slips past 06:30, you reship yesterday's
  table. Prefer an explicit Dataset sensor over "hope the cron
  order holds".
- `end` / `end_{country}` use `ALL_DONE`. A failed batch does not
  block later markets — good for partial delivery, bad if you assume
  "DAG green == every partition shipped". Watch failed-task count.
- Event ingest is additive. Re-runs re-post the same rows —
  coordinate with the consumer before a historical replay.
- Avro marks `table_PII: no` in the registered schema, but the feed
  includes business listing phones / emails. Treat those columns as
  sensitive in transit and at rest even if the schema flag is soft.
- Chunk size is 1000 (wide rows: nested JSON stringified). If the API
  starts 413-ing, drop `CHUNK_SIZE` before rewriting the encoder.
- `max_active_tasks=5` equals `TOTAL_BATCHES` so one country saturates
  the pool; the next waits on `end_{cc}`. Intentional.

## What this is not

Not ranked menu-gap opportunities (patterns 12/14). Not peer spend
gaps (pattern 16). Not FBO/NBO scoring (pattern 04). Not the
foodgraph refine SQL that *builds* the refined market-data tables —
only the export path.
