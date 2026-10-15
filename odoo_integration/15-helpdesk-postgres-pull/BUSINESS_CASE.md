# Business case: Odoo helpdesk Postgres incremental pull

Pattern 06 already ships yesterday's helpdesk creates from refined
BigQuery to a partner event bus. That assumes the warehouse already
has clean Level-1 helpdesk tables. Somebody has to land the raw Odoo
rows first.

This DAG is that landing path. It reads Odoo Postgres directly
(psycopg2, SSL required), writes NDJSON onto the Composer data volume,
copies into the raw zone under a dated prefix, truncates staging, and
kicks a dbt Cloud job to rebuild trusted helpdesk models. Tickets use
a rolling two-day create/write window so late-night edits do not miss
the cut; dimension tables (team, type, medium, stage, tag, tag-rel)
are full refreshes — they are small and truncate is honest.

## What this unlocked

- A reusable Postgres-pull shape for helpdesk that did not depend on
  OdooRPC throughput or XML-RPC payload limits
- Staging tables that dbt could trust as the contract for Level-1
  models feeding pattern 06 and internal SLA dashboards
- On-demand runs after Odoo cutovers without inventing a separate
  backfill tool

I kept mail_message off the default table list. Conversation bodies
are PII-heavy and large; enable that extractor when a concrete
downstream needs thread context, not "just in case."

## Constraints

- `schedule_interval=None` in the source — triggered manually or by an
  external scheduler. Do not silently turn this into a noisy hourly
  without checking Odoo connection limits.
- Table triples run sequentially (`fetch → GCS → BQ` per table). Easy
  to reason about; slow when Odoo is under load. Parallelize only
  after you know how many concurrent Postgres sessions the ERP allows.
- Ticket extract is not a hist/SCD load — staging is WRITE_TRUNCATE
  for every table in the default list. Downstream dbt owns history.
- Portal `access_token` is dropped from the sanitized ticket extract.
  Do not reintroduce it into landing zones.

## What this is not

Not pattern 06 (refined → event bus). Not an OdooRPC write path
(patterns 01–03). Not the archived multi-wave helpdesk migration
loaders. This DAG stops at "raw helpdesk entities are in staging and
dbt has been asked to refresh trusted models."
