# Business case: Establishment attribute Deepideas export

Account and assortments teams need a stable picture of *who* the
active wholesale buyer is as an establishment: how close to the next
store, how dense the competitive set, how digital, what cuisine, what
menu mix. That profile sits beside peer spend gaps (pattern 16) and
public market listings (pattern 17). Mixing the three contracts made
partner schemas brittle and made ops argue about full-reload vs delta
for the wrong reasons.

This pattern materializes the enrichment snapshot in BigQuery staging
and ships only hash-delta rows to the partner event bus.

## What this unlocked

- One weekly Composer path that rebuilds "today", POSTs the delta,
  then advances the yesterday snapshot — same delta contract as other
  Deepideas siblings, without forcing gaps_category / ingredients into
  the same reviewable sample
- A single row per wholesale buyer (`ROW_NUMBER` tie-break) so the
  partner does not see duplicate customer keys when multi-source
  establishment joins fan out
- `_keyhash` / `_rowhash` so a quiet week costs almost nothing on the
  HTTP side; a noisy week still reuses the same Avro schema
- Clear separation from purchase-gap arrays and from the wide listing
  document that prefers monthly full load

## Constraints

- Active-buyer filter (last transaction within a year, status, branch
  groups) is the SLA boundary. Do not export the full establishment
  universe — partner ingest and Composer slots both feel it.
- Attribute join is multi-source (refined establishments, discovery
  geo fallback, trusted market-area stats, digitalisation view, menu
  items). Failures are usually upstream freshness, not Avro encoding.
- Partner Avro types mix ints and strings intentionally. Cast in the
  send SELECT; do not "clean up" types in Python unless the registered
  schema changes.
- Production ran establishment beside gaps_category and
  gap_ingredients in one sequential DAG. Isolating establishment here
  is deliberate for the portfolio — sibling patterns should stay
  separate so each contract stays reviewable.

## What this is not

Not peer benchmarking gaps (16). Not market-listing monthly full load
(17). Not menu-gap opportunity ranking (12/14). Not a real-time
scoring API. This stops at "weekly enrichment attributes for active
buyers land on the partner bus when the row hash changes."
