# Business case: Odoo helpdesk tickets daily event export

Support ops and partner analytics need Level-1 helpdesk opens from Odoo —
ticket type, medium, escalation, status, store linkage — as daily events.
They do not need a full dump of the helpdesk table every night.

This pattern is the thin bridge: let dbt refresh the refined helpdesk
model, select yesterday's creates in BigQuery, and push that slice as Avro
events to an external ingest API. One market when this shipped; the DAG
wiring stays linear on purpose.

## What this unlocked

- Daily Odoo helpdesk → event bus without replaying open + closed history
- A create_date delta that matches how support analytics answers
  "what opened yesterday" without standing up CDC on Odoo's Postgres
- Explicit dbt-before-ingest ordering so the refined table is never a
  half-built morning snapshot

I cared more about the schedule and the filter than about clever hashing.
Helpdesk tickets are mostly append on create; status updates live
elsewhere for ops tooling. Hash-delta (as in the SFDC asset export) would
have been overkill here and would have forced a history side table we did
not need for this consumer.

## Constraints

- Schedule is daily (`30 4 * * *`), after overnight Odoo → warehouse
  landing. Weekly would miss the same-day support volume reviews that
  ops runs each morning.
- Country is hardcoded to one market (`de`). Extending means a country
  list and either a partitioned refined model or per-country filters —
  not inventing multi-country wiring that production never had.
- Avro bulk posts in chunks of 500 to match the shared event-API contract.
  Ticket volume for one market is small; chunk size is consistency, not
  throughput.
- dbt job timeout is 300s. Sibling DANA exports used 600s; this model was
  lighter when it shipped. Watch it if the refined model grows joins.

## What this is not

Not an Odoo → warehouse extractor (that lives upstream of the refined
table — see the helpdesk pull patterns separately). Not ticket write-back
into Odoo. It is warehouse → event bus for yesterday's Level-1 opens.
