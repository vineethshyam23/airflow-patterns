# Business case: Matching engine export to partner event bus

Wholesale markets needed a weekly feed of which SaaS products each
matched customer holds — not the SCD history of how those matches
evolved. Pattern 01 already maintains `match_result` as Type 2. This
pipeline consumes the *current* valid matches, joins wholesale
attributes and the establishment product footprint, and ships one Avro
row per (customer, service) to a partner event bus.

I kept prepare and ingest in one DAG. The staging table is a full
replace; starting ingest before prepare finishes is how you ship
last week's DE rows with this week's FR label. One chain makes that
ordering obvious in the graph.

## What this unlocked

- Multi-country feed (13 markets) from one staging rebuild
- Product flags unpivoted into service rows the partner schema expects
- Inactive establishments flagged via service code + 700 instead of a
  separate status dimension on every product row
- Weekly cadence — match quality does not move fast enough to justify
  daily BQ + API spend

## Constraints

- Match quality cutoff is `<= 150`. Looser matches stay out of the
  feed by design; changing it is a product decision with the partner.
- Dedup keeps one internal establishment per wholesale id (highest
  activity, then oldest service start). Multi-site wholesalers that
  legitimately map to several establishments lose the rest in this
  export — that matched the consumer contract when this shipped.
- Full country result sets are buffered in memory for Avro encode.
  Fine for weekly multi-market volume; revisit if this ever goes
  hourly.
- Event ingest is additive. Re-runs re-post the same rows — coordinate
  with the consumer before a historical replay.

## What this is not

Not the SCD Type 2 matcher itself (pattern 01). Not a real-time CDC
stream. Not the Odoo / SFDC lifecycle deltas (patterns 05 / 09). This
DAG stops at "current valid matches are shaped as service rows and on
the event bus."
