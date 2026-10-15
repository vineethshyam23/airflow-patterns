# Business case: Dining Guide DQ-gated multi-env publish

Consumer apps do not tolerate a quiet warehouse regression. If
yesterday's establishment universe shrinks 20%, or Google Places
coverage collapses, the product team finds out from store reviews —
not from a green Airflow square. This DAG is the publish valve between
a dbt trusted build and four Dining Guide BigQuery projects.

## Problem

The Dining Guide mart is rebuilt daily from ERP + market + product
signals (dbt Cloud). Publishing that mart into app projects with a
blind `WRITE_TRUNCATE` is cheap and wrong: a bad upstream join once
wiped large slices of cuisine and location coverage before anyone
noticed. We needed a gate that compares today's trusted cut to
yesterday's live prod snapshot and blocks the fan-out when proportions
or averages move past a threshold.

## Approach

1. **Build** — deferrable dbt Cloud job materializes
   `trusted.dining_guide_data_base`.
2. **Anonymize** — Blake3 short hash of `establishment_id` becomes
   public `dine_id` (base64url, 10 chars). Hash map is a truncate
   reload, not a join-time UDF.
3. **Compare** — one monitoring row with 30+ boolean flags: each flag
   is TRUE when today's metric degrades more than 15% vs yesterday's
   consumer-app prod table (test establishments excluded).
4. **Branch** — fail closed to Slack; pass path backs up the prior
   trusted push, then `WRITE_TRUNCATE`s into every configured app
   project. A ratings-enriched sibling table publishes to dev only.

I kept the gate in Composer rather than dbt tests. dbt can assert
null rates inside the warehouse; it cannot (easily) FULL JOIN against
the consumer-app prod project that received yesterday's push, then
stop four cross-project loads and page the product Slack. That
orchestration belongs next to the publish tasks.

## Tradeoffs

- **Threshold is blunt.** A real market exit can trip the same 15%
  flags as a broken join. Operators read the monitoring row and
  either fix upstream or temporarily relax a noisy column (production
  has done this for Instagram coverage). Fail closed is still cheaper
  than an unattended bad publish.
- **Hash on the worker.** Pandas + Blake3 is simple and auditable;
  at larger scale move the hash into BigQuery SQL.
- **Full truncate per env.** The trusted table is the contract.
  Incremental MERGE into app projects was rejected — product prefers
  atomic daily replace over partial deltas.

## Distinct from nearby patterns

| Pattern | Relationship |
|---------|--------------|
| 40 Food Graph multi-project propagation | ML gold/stage copy, no DQ valve |
| 32 Invoice Radar | Finance reconciliation report, not app publish |
| 67 Daily refined SCD spine | Warehouse SCD, not consumer-app cutover |
| Reservation Tool land (#54) / centralization | Upstream RT sources; not this publish gate |
