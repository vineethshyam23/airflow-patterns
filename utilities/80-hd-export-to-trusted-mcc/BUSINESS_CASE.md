# Business case: HD export → trusted MCC materialization

Wholesale / MCC consumers needed a stable, queryable copy of
per-country `export_*` views that live in a separate ATS export
project. Those views are fine for ad-hoc pulls; they are a poor
contract for dbt and BI that expect tables in the DWH project with
predictable names and overnight freshness.

I kept this as its own daily DAG rather than folding it into the
wholesale NL API land (patterns 69/70) or the refined-zone SCD spine
(pattern 67). Different source project, different object shape
(views → tables), and a reporting contract that has to survive
partial country failures without losing the rest of the fan-out.

## What this unlocked

- One parallel fan-out: seventeen country tasks after `start`, each
  listing and materializing its own `export_{cnt}` dataset
- Destination naming `{cnt}_{table}` so country grain is in the
  table id and consumers do not need seventeen datasets
- Soft failure per view: one broken private-label view does not
  abort the country; failures return as plain XCom lists
- Aggregated Slack via the Data Platform bot (`chat.postMessage`)
  instead of a webhook + base64 XCom payload — easier to audit and
  invite to a channel
- Known permission-denied views filtered from Slack noise so on-call
  is not paged for objects we already accept as unreadable

## Constraints

- Schedule is `0 2 * * *`. Upstream export-project refreshes must
  finish before 02:00 or the truncate copies yesterday's view
  definition against stale base tables.
- Full WRITE_TRUNCATE each run. No watermark. Re-runs overwrite the
  trusted tables for that country — fine for a daily snapshot,
  wrong if anyone treats these as append-only history.
- `prepare_failed_views_for_slack` uses `ALL_DONE`. A hard-failed
  country task still produces a Slack summary; do not read a green
  Slack "Success" as seventeen-market coverage without checking the
  message body.
- Worker needs read on the export project and write on
  `dwh_project.trusted_mcc`. Missing dataset list → country skipped
  (empty XCom), same as the bash script this replaced.

## What this is not

Not wholesale NL MCC API / CHD CSV land (patterns 69/70). Not the
refined-zone SCD spine (pattern 67). Not a per-country dbt wrapper
(`etl_mcc_*_dbt`). This is cross-project BigQuery view
materialization with parallel country tasks and aggregated Slack
failure reporting.
