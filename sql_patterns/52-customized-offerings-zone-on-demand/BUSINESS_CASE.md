# Business case: Offer Tool on-demand zone publish

Field sales opens the Offer Tool against product-owned BigQuery projects,
not the warehouse. Pattern 48 already covers the *scheduled* weekday
publish of gaps, assortment, scores, and recommender tables. That DAG is
heavy: Wednesday triples the stage fan-out, and a mid-day product ask
should not wait for the next scheduled slot or re-run the full graph.

This pattern is the lean manual twin. Trigger it when establishments,
search index, or catalog tables need a fresh cut *now* — after a refined
Food Graph backfill, a soft-delete cleanup, or a go-live dry-run into
acc/stg — without paying for assortment / FBO / article-recommendation
rebuilds you do not need.

## Problem

Product and ops regularly need:

1. A broader establishment footprint than the scheduled zone ships in one
   pass (markets where cards exist but Elasticsearch is not wired yet).
2. A search-index projection for the markets that *do* have ES.
3. Catalog / geo / mapping refreshes that are independent of ML gap nests.

Bundling that into pattern 48 would either bloat every scheduled run or
force operators to clear unrelated failed tasks after a partial trigger.
A second DAG with `schedule_interval=None` keeps the cost model honest.

## Approach

- **Manual cadence**: `schedule_interval=None`, `max_active_runs=1`.
- **Same Wednesday stage policy** as #48 at parse time, so an on-demand
  Wednesday run still widens to acc + stg + prod and does not leave
  acceptance stale relative to the scheduled publish.
- **Two country lists**: 16 markets for establishments; 11 for the
  Elasticsearch projection. Cartesian product is stage × country, no
  inter-task barriers — product tolerates partial country freshness.
- **Lean table set**: establishments + FG ingredients / images / article
  mapping + ingredient recommendations + zip geo + wholesale stores +
  soft-delete-filtered mappings + menu sources. No gaps validation chain,
  no Monday SoftCircuit, no assortment / score / recommender families.

## Tradeoffs

| Choice | Why |
|--------|-----|
| Separate DAG vs parameterizing #48 | Operators trigger by intent; failure surface stays small |
| Parse-time Wednesday stages | Matches #48 destination set; downside is stage list frozen until DAG reparse |
| No task dependencies | Parallel truncates finish faster; one failed country does not block others |
| Soft-delete filters on mappings only | Establishments still rely on `offer_tool_relevant`; mappings are where deleted cards leak into UI |

## Not claimed

No invented slot-cost savings or team-size numbers. The value is
operability: a named trigger for the surfaces product actually asks to
refresh between scheduled publishes.
