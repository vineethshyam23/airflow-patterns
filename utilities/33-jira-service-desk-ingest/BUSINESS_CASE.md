# Business case: Jira Service Desk ingest

Support ticket history is the closest thing we have to a continuous
signal on product friction. HDSD / POSAPP-style projects hold SLA
breaches, escalation chains, and the free-text that never makes it into
CRM fields. Finance and CX leadership wanted that history in the
warehouse next to subscription and POS facts — not trapped in Jira
filters that expire when someone leaves.

## Problem

A one-shot CSV export does not survive schema drift (ADF descriptions,
custom fields, changelog). Re-pulling years of issues in a single API
call hits rate limits and Composer task timeouts. Teams that only kept
an incremental feed discovered they could not rebuild after a bad dbt
model or a corrupted staging load.

## Approach

Treat Jira like any other brittle SaaS source:

1. **Land raw JSONL** — one issue per line, description/comments
   flattened from ADF, changelog kept nested. Staging is a single JSON
   column so field renames do not break the load.
2. **Two operating modes on one DAG** — incremental twice daily for
   production; monthly TaskGroup fan-out for full history, sized from
   the project's real created→updated span at parse time (same idea as
   Odoo EDI rank splits).
3. **dbt after append** — BigQuery gets WRITE_APPEND; dedupe and
   normalization live in dbt, not in the extract.

I kept credentials behind an Airflow Variable resolved inside the task.
Parse-time Variable reads for secrets caused rotating-token outages
more than once.

## Why this shape

Monthly shards isolate API failures: one bad month retries without
re-pulling 2018–2024. The merge step is boring file concatenation on
purpose — easier to reason about than XCom payloads of multi-GB JSON.
The cost is parse-time API calls when FULL_LOAD_MODE is on; acceptable
for a rare backfill, wrong for the twice-daily path (which never calls
the date-range probe).
