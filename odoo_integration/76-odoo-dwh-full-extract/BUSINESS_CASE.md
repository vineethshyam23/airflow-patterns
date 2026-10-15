# Business case: Odoo DWH full extract

Odoo is the system of record for CRM, subscriptions, helpdesk, partners,
and a chunk of finance. Downstream exports (partner event bus, country
packs, reverse ETL) and BI marts all assume a fresh trusted layer. That
trusted layer has to be built somewhere — this DAG is that somewhere.

## Why a full daily spine

Early on we tried domain-only pulls (helpdesk here, sale orders there).
It looked cleaner on paper and fell apart in practice: shared dims
(`res_partner`, stages, tags) drifted, dbt models crossed domains, and
on-call could not answer “is Odoo landed?” with one DAG status. A single
inbound spine with a clear success signal beats a dozen partial lands.

Full replace on staging (`WRITE_TRUNCATE`) is blunt but honest. For dims
and for tables where Odoo’s `write_date` coverage is uneven, truncate +
reload is cheaper than maintaining SCD contracts in the land layer. The
expensive tables (`sale_order*`, `crm_lead`, `helpdesk_ticket`,
`res_partner`) use a create/write lookback so we are not scanning the
entire history every night — still not true CDC, but enough for daily
trusted rebuilds.

## What this pattern is not

It is not the helpdesk-only pull (pattern 15). It is not an outbound
event export (06–14). It is not the sale_order_line snapshot soft-close
(72) or delivery reverse ETL (60). Those sit on top of the trusted
tables this job produces.

## Production constraints that shaped the design

**Replica stampede.** ~105 concurrent Postgres sessions from Composer
workers took the replica down. Cap with `max_active_tasks` and accept
that the DAG wall-clock grows.

**DagBag parse timeout.** Building a TaskGroup *inside* a 105-iteration
loop at import time exceeded Composer’s import timeout and the DAG
vanished from the UI. One TaskGroup at parse time, tasks created in a
plain loop, fixed it.

**Composer FUSE.** Writing NDJSON straight onto the mounted GCS volume
raced with loaders. Extract to local `/tmp`, `fsync`, then
`GCSHook.upload`.

**Stuck dbt HTTP.** A hung Cloud trigger once held `max_active_runs=1`
for days. `dagrun_timeout` on the DAG plus `execution_timeout` on each
deferrable dbt task are the backstops.

## Tradeoffs I would still revisit

Incremental lookback of one day means a missed run needs a catch-up
plan — raising the window without one just silently widens cost. The
25 parallel dbt jobs can contend with other 01:00 workloads; consolidating
domains would reduce Cloud concurrency pressure at the cost of longer
critical path. Row-count reconciliation Odoo↔BQ was never built; dbt
tests catch schema issues, not silent under-extracts.
