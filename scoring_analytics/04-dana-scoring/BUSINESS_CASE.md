# Business case: multi-country FBO/NBO scoring export

Sales and product teams need a ranked view of which establishments to pursue
next — first-best-offer for prospects not yet in CRM, next-best-offer for
accounts that already have a CRM establishment id. The scoring models live in
BigQuery. Downstream systems (CRM lead engines, partner portals) consume
events, not warehouse tables.

This pattern is the bridge: materialize a monthly cross-country scoring
snapshot, detect what actually changed since last month, and push only the
delta as Avro events to an external ingest API.

## What this unlocked

- One monthly job covering a dozen markets instead of per-country scripts
- Hash-based change detection (`_keyhash` / `_rowhash`) so quiet months cost
  almost nothing on the API side
- A history table that keeps superseded score versions for audit without
  replaying the full universe every run

I cared about the ordering constraint more than the SQL itself. The history
table has to stay frozen while the per-country ingest tasks run. If you
append/expire hist first, the delta query returns zero rows and the event bus
goes silent for a month — a failure mode that looks like success in the UI.

## Constraints

- Schedule is monthly (`55 0 10 * *`), not daily. Scoring models refresh on a
  slower cadence; daily would mostly re-send identical payloads.
- Whitelist filters drop blocked / checkout-flagged customers before anything
  leaves the warehouse. That is a business rule, not a performance trick.
- Avro bulk posts in chunks of 500. Large markets (DE, FR) still finish in
  minutes; the 401-retry on the OAuth client matters more than chunk size
  when a long country loop outlives a token.
- One market (PL) sits on the send list without a country model — empty
  result is intentional, not a bug.

## What this is not

Not the scoring model itself. Not CRM write-back. It is warehouse → event
bus for potential-customer scores, with an SCD-style history side effect so
we can prove what left the platform each month.
