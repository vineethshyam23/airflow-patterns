# Business Case: Adobe Analytics hourly Data Feed land

Product and growth analytics needed hit-level Adobe Data Feed rows in
the warehouse within the hour — not the next morning's batch report.
Adobe drops compressed packs into a landing bucket; the DWH owns
unpack, schema load, dimension decode, and a refined table analysts
can join without memorizing Adobe's integer codes.

I kept the unpack step on Composer workers rather than a Cloud Function
or Dataflow job for three production reasons:

1. **Dual payload shape** — the same hour can land `.tar.gz` lookup
   packs *and* `.tsv.gz` hit files. One Python path that lists,
   decompresses, renames with the suite stem, and moves to processed
   is cheaper to operate than two transfer configs that drift.
2. **Lookup fan-out is graph work** — thirteen key/value dumps need
   stage → distinct → trusted TRUNCATE in parallel, then a barrier
   before the refined enrich. Airflow TaskGroup-style loops express
   that; a single BQ load job does not.
3. **Hourly idempotency without a full rewrite** — refined appends
   with `hit_id NOT IN (last calendar day)`. That is a pragmatic
   compromise: cheap on quiet hours, correct enough when Adobe
   redelivers a file, and bounded enough that we do not scan the
   entire history table every run.

## What this unlocks

- Trusted hit append (`aa_hit_data`) for audit / replay.
- Truncated lookup dimensions (`aa_browser`, `aa_os`, …) always
  matching the latest feed dump.
- Refined `analytics_datafeed` with visit/hit IDs, valid_hit, hashed
  IP, and decoded exclude_hit / hit_source / referrer_type labels.
- Downstream dbt / BI that never joins raw Adobe integers directly.

## Tradeoffs I accepted

- `max_bad_records=50000` on the hit TSV load. Adobe feeds are wide
  and occasionally jagged; failing the hour on a handful of bad rows
  burned more SLA than the bad-row budget. Lookups stay at zero.
- Trusted hit load is WRITE_APPEND with lineage columns, while
  lookups are WRITE_TRUNCATE. Lookups are small and must stay
  consistent with the feed; hits are the growing fact.
- Refined SQL still lives next to the DAG (extracted to
  `refined_hit_query.sql` here). The app-suite sibling later moved
  transforms into dbt Cloud — good for that path; the web hourly
  path kept inline SQL while the lander was still changing.
- Worker local disk under `/tmp` for unpack. Fine at hourly volumes;
  if packs grow past worker ephemeral disk, stream to a staging
  prefix instead of extracting locally.

## Not this pattern

- Pattern 25: SEO listing NDJSON GCS ingest (different vendor contract).
- Pattern 38: POS vendor GA4 rolling ingest (Data Transfer + 7-day
  DELETE+INSERT).
- App-suite rawfeed / dbt-migrated app job — same extract shape,
  different prefixes and SQL ownership; leave for a later pattern if
  the delta is still material after this one.
