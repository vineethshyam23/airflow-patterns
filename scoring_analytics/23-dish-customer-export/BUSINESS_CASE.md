# Business case: Multi-country platform-customer footprint export

Wholesale reporting and partner analytics needed one reliable answer
per country: which matched customers run which platform products
(bundle tier, Reservation, Website, Order, POS, Pay), when those
products were activated or deleted, and who referred them. That is
not a KYC status feed (pattern 11), not a monthly market listing dump
(pattern 17), and not a scoring model (pattern 04). Collapsing it
into a one-off spreadsheet export broke every time a market asked for
a new flag.

This pattern builds a consolidated staging table from 14 country
queries in parallel, lets dbt refine it into a valid-flag snapshot,
then ships today's rows to the partner event bus as Avro.

## What this unlocked

- One Composer path that fans out country inserts, waits on a single
  dbt job, then fans out ingest — instead of 14 nearly-identical DAGs
  that drift apart the first time someone patches Spain only
- A matching contract that prefers CRM-cleaned wholesale↔establishment
  links, falls back to fuzzy `match_result`, and still picks up POS
  customers matched through a secondary establishment source. Messy,
  but it matched how identity actually worked in the warehouse
- Truncate-then-append into one staging table so dbt sees a single
  multi-country input. First country owns WRITE_TRUNCATE; everyone
  else appends. Racey if you reorder the list without thinking —
  leave the first element alone or make truncate explicit
- Partner-facing Avro with creation/deletion timestamps per product.
  Acquisition and churn reporting stop asking for ad-hoc extracts

## Constraints

- Fourteen parallel BigQuery inserts share one destination table.
  Composer parallelism is fine; what bites is a partial failure after
  truncate — you can land with half the countries. Retries help; a
  row-count gate before dbt would help more (we did not ship that in
  production and I still miss it)
- dbt sits in the critical path with a 300s timeout. The job has to
  finish before any ingest task starts. If it slips past the partner
  reporting window, the whole day's feed is late — not just one
  country
- Send query filters `_valid_from >= current_date()`. That is a
  "today's valid rows" contract, not a hash-delta. Quiet days still
  re-ship the current footprint for each country. Partner ingest must
  tolerate that, or you need a delta layer like patterns 20–22
- HR has an extra `status_cd = 1` filter. Country quirks belong in SQL,
  not in fourteen copy-pasted Python branches
- BE is mapped in the query module but was never on the export country
  list. Do not "fix" that silently — it was a product decision

## What this is not

Not payment KYC (11). Not market listing enrichment (17). Not FBO/NBO
scoring (04). Not MAG acquisition/penetration aggregates (those are a
separate, thinner reporting pair). This stops at "matched wholesale
customers' product footprint for 14 countries lands on the partner
bus after a dbt refine."
