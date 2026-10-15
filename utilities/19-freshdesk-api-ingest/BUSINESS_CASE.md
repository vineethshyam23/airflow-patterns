# Business case: Freshdesk REST API ingest

We already pull Odoo helpdesk from Postgres (pattern 15) and push
refined tickets to an event bus (pattern 06). A separate POS / support
tenant still lived in Freshdesk — different product, different API,
same warehouse expectation: land raw entities, truncate staging, let
dbt own trusted models.

This DAG is the Freshdesk landing path. It paginates REST list
endpoints into NDJSON on the Composer data volume, copies into the raw
zone under a dated prefix, loads BigQuery staging, and kicks dbt Cloud.
Tickets run hourly with an `updated_since` window from the first of the
month. Dimension resources (contacts, agents, roles, groups, companies)
run once a month on the 1st at 01:00 via a BranchPythonOperator so we
do not burn API quota re-pulling slowly changing dims every hour.

## What this unlocked

- One Composer DAG for both cadences instead of two schedules that
  drift out of sync
- Stable staging contracts for Freshdesk entities that marketing and
  support ops could join against Odoo tickets without inventing a
  second helpdesk warehouse
- Bounded ticket extracts — month-to-date `updated_since` keeps hourly
  pages from scanning the whole ticket history

## Constraints

- Freshdesk list pages are hard-capped (`per_page=100`). Deep history
  means many sequential pages; retries restart from page 1 and rewrite
  the NDJSON file. That is fine for month-to-date tickets; do not point
  this client at a full historical backfill without a cursor strategy.
- Monthly branch uses `ALL_SUCCESS` into `pause` then one shared dbt
  job. A failed contacts fetch blocks the dims dbt refresh — intentional
  so trusted models do not rebuild on a partial dim set.
- Hourly ticket path has its own dbt job with `ALL_DONE` so a soft
  failure still reaches `end` and alerting can see the run close.
- `custom_fields` on contacts/companies are stringified before NDJSON
  write. Nested objects break the flat `schema_json` loads we use for
  staging. Downstream dbt can `PARSE_JSON` if a field matters.

## What this is not

Not pattern 15 (Odoo Postgres). Not pattern 06 (event export). Not a
Freshdesk write-back or ticket sync into Odoo. Stops at "Freshdesk
entities are in staging and dbt has been asked to refresh."
