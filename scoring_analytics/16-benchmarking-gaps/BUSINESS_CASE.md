# Business case: Peer benchmarking gaps (multi-country)

Sales teams ask a blunt question: for this establishment, which
category spend is below peers in the same segment, and by how much?
That is not a menu-gap ranking (patterns 12/14) and not an FBO/NBO
score (pattern 04). It is a peer purchase comparison over a rolling
year of wholesale transactions.

This pattern materializes that answer per country in BigQuery, then
optionally ships a flat category-level slice to a partner event bus
via Avro bulk ingest.

## What this unlocked

- One Composer DAG that fans out the same diamond of BQ jobs across
  markets with reliable taxonomy coverage, without copy-pasting SQL
  per ISO code
- Intermediate tables (topsellers, skeletons, establishments,
  transactions) that analysts can inspect when a gaps row looks
  wrong — the final ARRAY nest is not a black box
- Customer potential bands (approx quantiles of absolute gap) so
  account managers get a 1–5 signal, not just a pile of category
  deltas
- A reusable today/yesterday `_keyhash` / `_rowhash` delta contract
  for the export path, so weekly partner feeds do not re-send the
  full DE table every run

## Constraints

- Country taxonomy forks in SQL (PL/NL → MGE columns + stratbuy
  domain; others → PCG + catman). Hide that fork in the query module,
  not in the DAG body.
- Skeletons depend on topsellers for the same ISO. Establishments and
  transactions are independent until the final gaps join. Do not
  serialize the whole country into one giant query — the diamond
  exists because slots and debuggability both matter.
- Gaps SQL UNNESTs nested arrays and computes percentiles. It is
  expensive. Daily schedule at 05:45 assumes overnight refined loads
  finished; `max_active_runs=1` and `catchup=False` are not optional
  niceties.
- The partner Avro schema is a *flat* category comparison, not the
  nested refined gaps table. Export SELECT must project and cast to
  strings the ingest contract expects.

## What this is not

Not menu-gap opportunity ranking (12/14). Not Deepideas establishment /
category / ingredient sibling exports (those are separate feeds on the
same weekly DAG in production). Not a real-time scoring API. This
stops at "refined peer-gap tables exist per country, and a delta can
be Avro-posted when the partner feed is enabled."
