# Business case: Odoo WSL invoices dual export

Wholesale (WSL) invoice lines from Odoo are the spine for two different
consumers that need the same fact table on a daily cadence:

1. An external event bus that feeds partner / finance analytics for a
   pilot market
2. An internal ML recommender history table that wants a growing append
   of billed product × establishment rows

I did not want two Composer DAGs racing the same dbt job and drifting on
filter logic. One DAG owns the refresh, then fans out: APPEND for ML,
Avro bulk for the event API. Same SELECT, same `uni_key IS NOT NULL`
gate, same day.

## What this unlocked

- One trusted intermediate (`int_odoo_wsl_invoices`) as the contract for
  both event ingest and recommender training history
- Daily freshness after the Odoo → warehouse extract without standing up
  Odoo CDC
- A place to hang ops runbooks: if dbt fails, neither consumer moves; if
  the API flakes, recommender history is still current

This is different from the list-price / commission pattern (07). That one
is a monthly hash-delta of partner commission measures. This one ships the
wholesale invoice fact table itself — amounts, products, establishments,
geo — and deliberately re-sends the full current snapshot to the event bus.

## Constraints

- Pilot market is DE only. Invoice semantics and agency filters differ
  enough that a second country is an explicit add, not a loop day-one.
- Full-table send (`SELECT * WHERE uni_key IS NOT NULL`). Cheap early;
  volume grows with billing history. When cost bites, add a booking_date
  window — do not invent CDC mid-flight.
- Recommender copy is `WRITE_APPEND`. Catchup-on-scheduler-gap equals
  duplicate daily snapshots. Sanitized DAG keeps `catchup=False`;
  production originally had `True` and paid for it on backfills.
- Event ingest is additive on the bus. Re-runs re-post the same rows —
  coordinate with the consumer before a full historical replay.

## What this is not

Not the Odoo Postgres extract (that is `wsl_extract` / OdooRPC upstream).
Not commission settlement math (pattern 07). Not a Vertex AI training
job — the APPEND lands in a BigQuery table the recommender stack reads
later. This DAG stops at "trusted facts are fresh in both sinks."
