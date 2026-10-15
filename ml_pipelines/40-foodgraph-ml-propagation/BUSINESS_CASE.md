# Business case: Food Graph ML propagation

## Problem

Menu-ingredient ML does not live in the warehouse. Preprocessed
tables land in a Vertex AI project (gaps, articles→ingredients,
synonyms, synthetic menus). Downstream teams — recommender ranking,
offer tooling, payment-wallet matching — need those outputs in the
main DWH on a daily schedule, plus a month-end ranked gaps snapshot
in the recommender project. Doing that as ad hoc BQ scripts meant
missed countries, silent schema drift on partition copies, and no
gate between “always on” gold tables and “run once when ML asks.”

## Decision

One Composer DAG owns the cross-project contract:

1. **Gold reference tables daily** — translations, ingredient
   hierarchy, synonyms, drink classification → trusted dataset.
2. **Stage propagation** — copy Vertex `dev` and `acc` preprocessed
   tables into trusted with WRITE_TRUNCATE so analysts see both
   stages without touching Vertex IAM.
3. **ShortCircuit for calendar and ops** — month-end ranked gaps only
   fire the day before month close; acc-promotion tasks stay behind
   an on-demand Variable so a code edit is not required.
4. **Fixed-column partition copy** — ranked gaps land in the
   recommender project with an explicit SELECT list. `SELECT *`
   broke partition writes the first time ML added columns.
5. **Payment-wallet match → dbt** — matching-engine output lands in
   staging, then a dbt Cloud job; run ids are stored on a Variable
   for downstream observability.

This is not the Avro export to the partner event bus (pattern 12) and
not the market-data extract (pattern 17). Those consume tables this
DAG (or its cousins) produce. The engineering value here is
multi-project orchestration with calendar gates and a schema-stable
handoff.

## Constraints I cared about

- Three GCP projects, one DAG, `max_active_runs=1`. Cross-project
  copies are I/O heavy; uncontrolled parallelism is fine for
  independent TRUNCATEs, dangerous for the ranked-gaps fan-in.
- Ranking SQL joins ML gaps to one year of wholesale revenue and CRM
  account identifiers. That join is expensive and country-specific —
  run it monthly, not daily.
- Orphaned on-demand tasks in production ran every day with no
  upstream. Sanitized version wires only the gated tasks and drops
  the orphans.
- Production chained the partition copy off the last country in the
  loop. Fixed here: copy waits on all country loads.

## What I would not claim

No invented lift from menu-gap ranking or team-size savings. The win
is a reviewable multi-project schedule, fewer silent partition
failures, and a clear split between daily gold/propagation and
month-end ranking.
