# Business Case: Food Graph refined analytics zone

Wholesale card transactions arrive per country overnight. Product,
sales, and offering teams need a single refined layer that answers
"what did this establishment buy, how often, and what are the topsellers
in its peer branch?" — across ~16 markets — before customized-offering
and Food Graph ML jobs wake up.

I kept that contract in one Composer DAG rather than 16 country DAGs
or a pure dbt project. The reason is operational: the graph has a
hard sync barrier. Partitioned establishment history for 10 markets
must not start until every country `txn_for_analytics_*` table is
fresh. Airflow's Dummy/EmptyOperator as `loop1` is the cheapest,
most visible way to encode that. Slot reservation on the night-ETL
path matters too — 120+ WRITE_TRUNCATE jobs will thrash on-demand
pricing if you leave them unconstrained.

## What this unlocks

- Per-country article / visit / branch-topseller analytics at daily
  cadence, with currency stamped at query time.
- Global UNION ALL rollups for cross-market reporting without forcing
  every consumer to know the ISO fan-out.
- DAY-partitioned, `dwh_id`-clustered transaction history for the
  denser markets — the table shape downstream ML and offering tools
  already expect.
- A COP (customized offerings) masterdata feed that reuses the same
  refined dataset rather than inventing a second source of truth.

## Tradeoffs I accepted

- One large DAG (~120 BQ tasks) is harder to unit-test than dbt models.
  Versioned SQL in a module helps; it does not replace a warehouse
  test suite.
- WRITE_TRUNCATE everywhere is simple and correct for full rebuilds.
  It is also expensive on large countries (DE, FR, PL). Incremental
  merge is the obvious next cost lever once the barrier pattern is
  stable.
- Production left some global materializations loosely wired. I
  documented that here and only re-linked the edges that clearly
  depend on fan-in — better than silently inventing a perfect graph.

## Not this pattern

- Pattern 40: cross-project ML gold + ranked menu-gaps copy.
- Pattern 45: Vertex `PipelineJob.submit` with step flags.
- Pattern 27: Offer Tool Cloud SQL → SCD Type 2 ingest (different
  source system; consumes refined visit/article later).
