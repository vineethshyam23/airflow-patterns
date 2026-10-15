# Business case: POS Intelligence recommendations export

Field teams and partner analytics needed ranked *wholesale article*
recommendations per establishment — not another menu-gap score.
Upstream Vertex / foodgraph ML already computes POS usage vs
wholesale purchase gap and lands
`pos_article_final_recommendation_{CC}` in a preprocessed dataset.
This DAG is the export path onto the event bus.

I kept a monthly full load. The recommendation table is rebuilt by
the ML job each cycle; a row-hash delta would chase a moving
`article_rank` and `gap` that the partner already treats as a
snapshot. Full reship is cheaper to reason about than a brittle
fingerprint across ranked article lists.

France was the pilot market. Germany (and others) are a one-line
append to `COUNTRY_ISO_CODES` once the Vertex `_{CC}` table exists —
no DAG rewrite required.

## What this unlocked

- Partner-consumable Avro feed of POS-driven article recommendations
- Explicit type mapping (BQ INT64 → Avro long, FLOAT64 → double,
  BOOL → boolean, DATE → string) so the registered schema stays
  honest under schema-registry checks
- Auth that prefers `client_credentials`, falls back to password
  grant, and refreshes on 401 mid-load — monthly full loads outlive
  short-lived tokens
- Intentional omission of `menu_item_names` so we do not ship a
  column the v1 partner schema rejects

## Constraints

- Schedule is `15 6 1 * *` — same window as ranked menu-gap exports.
  The Vertex / foodgraph job that lands the source table must finish
  earlier that morning. Prefer a Dataset sensor over hoping cron
  order holds.
- `max_active_tasks=1` and countries chained sequentially. Parallel
  markets would double OAuth + ingest pressure for little gain on a
  single-pilot rollout.
- Chunk size is 500 (narrower rows than market-data listings). If
  the API 413s, drop `CHUNK_SIZE` before rewriting the encoder.
- Prod briefly pointed at the *dev* preprocessed dataset while ACC
  Vertex lagged. That TEMP switch is a known footgun — flip
  `SOURCE_DATASET` when ACC lands, and document the flip in the
  runbook.
- Event ingest is additive. Re-runs re-post the same rows —
  coordinate with the consumer before a historical replay.
- `end` uses `ALL_DONE`. A failed country does not freeze later
  markets once you add them — good for partial delivery, bad if
  "DAG green" is treated as full coverage.

## What this is not

Not ranked menu-gap opportunities (patterns 12/14). Not peer spend
gaps (pattern 16). Not FBO/NBO scoring (pattern 04). Not the Vertex
pipeline that *builds* the recommendation table — only the export.
