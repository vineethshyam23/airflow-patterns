# Business case: Ranked menu-gaps export to partner event bus

Wholesale field teams need ranked "what's missing from this
establishment's menu" opportunities — ingredient + article + relevance
+ trailing revenue — without opening BigQuery. The warehouse already
builds per-country refined tables via dbt; the partner wants those
rows on their event bus as Avro.

I kept dbt refresh and ingest in one DAG, and deliberately did *not*
fan out all countries in parallel. Eight markets × full refined tables
would hammer the ingest API and hide which country is the slow one.
Sequential countries with five hash-partitioned batches inside each
country was the compromise that kept wall-clock acceptable without
melting the sink.

## What this unlocked

- Monthly partner feed of ranked menu gaps without granting the bus
  warehouse access
- Stable parallelization via `FARM_FINGERPRINT(establishment_id ||
  article_no) MOD N` — no precomputed batch column to maintain
- Streaming BQ → Avro → POST so one fat country does not OOM the worker
- Same OAuth / Avro / chunk pattern as other event-ingest DAGs, so ops
  already know the failure modes

## Constraints

- Schedule is monthly (`15 6 1 * *`) but the export filter is D-1
  (`_update_ts` last 24h). That only makes sense if dbt itself is
  monthly (or you accept that mid-month changes wait for the next 1st
  *and* only rows touched in the last day of the month ship). If the
  business expects a full month of deltas, expose `full_load` via a
  DAG Param — do not silently change the filter.
- Chunk size is 2000 (wider rows than KYC/matching at 500). Stay under
  the API body limit; if payloads start 413-ing, drop CHUNK_SIZE before
  rewriting the encoder.
- `end` / `end_{country}` use `ALL_DONE`. A failed batch does not block
  later markets — good for partial delivery, bad if you assume
  "DAG green == every partition shipped". Monitor failed-task count.
- Event ingest is additive. Re-runs re-post the same rows — coordinate
  with the consumer before a historical replay.
- Avro schema marks `table_PII: no`. Account/person ids are present as
  opaque keys; no contact PII in this feed (the independent-
  establishment sibling is a different contract).

## What this is not

Not the FBO/NBO scoring hash-delta (pattern 04). Not the matching-
engine service export (pattern 10). Not the independent-establishment
menu-gaps variant (address/geo/contact schema — leave that for a later
pattern if it is still unused). Not the dbt models themselves.
