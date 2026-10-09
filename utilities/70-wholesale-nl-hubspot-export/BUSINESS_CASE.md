# Business case: Wholesale NL HubSpot reverse export

Wholesale NL enrichment lives in BigQuery discovery tables after the
inbound dual-source land and dbt models finish. Field sales and CRM
ops need those rows in HubSpot — prospects (unmatched establishments),
matched customers, and winning/losing id merge requests — without
waiting for a scheduled job that might fire against a stale snapshot.

I kept this DAG manual (`schedule=None`). Enrichment refresh is
event-driven on the analytics side; auto-scheduling the CRM push would
either race unfinished tables or re-blast HubSpot with yesterday's
snapshot. Ops triggers once the discovery suffix is confirmed.

## What this unlocked

- One Composer DAG covers three HubSpot-facing MCC endpoints under the
  same OAuth client used for inbound customer land
- 5,000-row chunks with a random `sessionid` per task — large enough to
  keep call count down, small enough that a mid-run failure does not
  lose the whole table in one POST
- Prospects and matched share one ~60-field payload shape; dedupe posts
  only winning/losing ids to the merge endpoint
- Snapshot table suffix moved to an Airflow Variable so enrichment
  refresh does not require a DAG deploy

## Constraints

- Full-table export every run. There is no watermark. Re-trigger can
  create duplicates in HubSpot unless the partner endpoint is
  idempotent — coordinate with CRM before a replay.
- Production originally swallowed API errors in a bare try/except so
  Composer could go green with a partial push. The sanitized helpers
  re-raise after logging; fail loud, then retry the task.
- Three tasks run in parallel with no edges. If the partner requires
  matched before dedupe, add `export_matched >> export_dedupe` — that
  ordering was never encoded in production.
- Payload includes establishment contact fields (phone, address, name).
  Treat as business PII in logging and support tickets.

## What this is not

Not the inbound MCC + CHD land (pattern 69). Not Avro partner event-bus
exports (patterns 10 / 11 / 53). Not Salesforce refined warehouse
fan-out (pattern 59). The dbt models that build
`data_enrichment_WholesaleNL_*` are out of scope here.
