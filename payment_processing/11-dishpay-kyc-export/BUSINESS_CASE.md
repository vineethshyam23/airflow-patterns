# Business case: Payment KYC export to partner event bus

Payment-product onboarding (KYC) status needs to land on a partner
event bus daily for one pilot market. The warehouse already owns the
raw KYC events and a dbt pipeline that historises them; the partner
only wants the *current* valid row per establishment — step, status,
duration, and PSP validation counters.

I kept dbt refresh and Avro ingest in one DAG. Shipping last night's
refined table after a failed SCD2 rebuild is how you report "approved"
establishments that flipped to error overnight. One chain makes that
ordering obvious.

## What this unlocked

- Daily partner feed without giving the bus direct BigQuery access
- Country + product scope enforced in dbt staging, not in the export
  SQL — export stays a thin SELECT → Avro → POST
- SCD Type 2 lives upstream; the bus gets `_valid_flag = true` only
- Same OAuth / Avro / chunk pattern as other event-ingest DAGs, so
  ops already know the failure modes

## Constraints

- Pilot is one country. Adding a market is a schema registration +
  consumer decision, not just appending to `PaymentKyc.countries`.
- Product filter (payment-now SKU) is applied in dbt. If someone
  broadens staging without updating the Avro contract, you will ship
  rows the partner cannot map.
- Full result set is buffered in memory before chunking. Fine for a
  single-market KYC table; revisit if this becomes multi-country
  hourly.
- Event ingest is additive. Re-runs re-post the same rows — coordinate
  with the consumer before a historical replay.
- Avro schema marks `table_PII: no`. Establishment ids and status
  flags only — no personal documents in this feed.

## What this is not

Not the Adyen Management API terminal inventory (pattern 03). Not a
real-time KYC webhook listener. Not the dbt models themselves — those
live in the warehouse repo; this DAG only triggers the Cloud job and
ships the result.
