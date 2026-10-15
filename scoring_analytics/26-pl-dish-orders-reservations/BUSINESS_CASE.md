# Business case: Single-market Order + Reservation lifetime export

Wholesale analytics for one market needed lifetime Order and
Reservation activity joined to the latest subscription asset — not
the multi-country product footprint (pattern 23), not MAG acquisition
dollars (pattern 24), and not FBO/NBO scores (pattern 04). Spreadsheet
pulls from the Order / Reservation product DBs drifted every month and
could not carry wholesale_id / store_id / owner contact consistently.

This pattern refreshes two refined tables with one tagged dbt Cloud
job, then full-loads both Avro contracts to the partner event bus on
a monthly cadence.

## What this unlocked

- One Composer DAG for two related product domains instead of twin
  DAGs that would diverge the first time someone patched Orders only
- Shared `_send_dataset` path: same OAuth, chunk size, retry, and
  ingest URL shape — only SELECT / Avro schema / schema id / row
  mapper differ. That kept Reservations from becoming a copy-paste
  fork of Orders
- dbt owns the join of transactional Order/Reservation facts to the
  latest subscription asset attributes. Composer only waits on the
  job, then ships. Changing commitment_period or asset_referrer logic
  does not require a Composer redeploy
- Partner gets a predictable monthly full reship. Quiet months still
  re-send the lifetime extract; ops cost was cheaper than debugging
  false deltas on wide owner+asset+order rows

## Constraints

- `max_active_tasks=1` serializes the two exports after dbt. Parallel
  fan-out is wired in the DAG graph, but Composer will not run both
  Python tasks at once under that cap. Raise it if the monthly window
  gets tight — watch BigQuery slot and partner ingest rate limits
- Owner email / phone / name travel in the Avro contract. The
  registered schema marked table_PII=no in production; treat the
  payload as sensitive in logging and support tickets anyway
- Full-load means every monthly run re-reads the entire refined
  table. Fine while the market is one country and row counts stay
  manageable; if a second market joins, revisit hash-delta or
  partition filters before cloning the DAG
- dbt sits on the critical path with a 600s timeout. If the tagged
  job slips, both exports are late — not just one product
- Single country today (`pl`). The `countries` list exists so a
  second market can be added without rewriting the DAG loop; do not
  pretend multi-country is already tested

## What this is not

Not pattern 23 (14-country product footprint with staging truncate).
Not pattern 24 (acquisition $ / penetration rates). Not an Odoo ERP
pull. This stops at "one market's Order + Reservation lifetime rows
land on the partner bus after one dbt refresh."
