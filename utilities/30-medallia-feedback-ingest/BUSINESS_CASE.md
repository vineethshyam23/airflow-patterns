# Business case: Medallia survey feedback SCD Type 2 ingest

Product and CX teams needed NPS, churn, and downgrade survey answers
in the warehouse next to establishment identity — not only as a
Tableau extract, but as a history they could join and re-slice.
Medallia exposes that through a GraphQL Query API with OAuth2
client-credentials. The Composer job's job is to land a rolling
year of feedback daily and keep Type 2 history when a respondent
changes an answer or a field mapping shifts.

I kept the extract + inline SCD2 shape rather than pushing
historization into dbt on day one. The DAG is six sequential tasks:
GraphQL → GCS CSV → staging truncate → hash insert → close obsolete
rows → promote tmp to trusted. That is older than the MCC country
pattern that moved SCD2 into dbt, but it still runs, and the hash
contract is easy to reason about when survey fields change.

## What this unlocked

- Daily trusted history of NPS / churn / downgrade responses
- Establishment-keyed identity (switched from user_id when the
  vendor started shipping establishment ids systematically)
- English machine-translation columns for free-text verbatims so
  analysts do not need per-language NLP for basic reporting
- A 366-day close window so ancient closed rows are not re-touched
  every morning

## Constraints

- `loaddate` and `oldest_record_allowed` are computed at DAG parse
  time (`date.today()`), not from the data interval. Fine on a
  stable 05:00 schedule; wrong for backfills and long scheduler
  restarts. Prefer `{{ ds }}` on a rewrite.
- Full 366-day GraphQL pagination every day. Cursor stops when the
  oldest page falls outside the window, but a quiet day still walks
  a lot of pages. Cap is 2000 iterations (~200k nodes).
- Pandas holds the whole extract in memory on the worker before the
  CSV upload. Fine at survey volumes; not a pattern for POS ticks.
- Production had no DEV/PROD project branch — one hardcoded project
  and bucket. Sample moves those to Variables.
- Records older than the lookback window are never closed by
  `data_update`, even if they disappear from the API.
- Survey free text is PII-ish. Treat the rawzone CSV and trusted
  table as restricted.

## What this is not

Not the outbound Medallia user / POS / establishment sync that
lives on Cloud Run / Cloud Functions — that is a different
ownership boundary (and out of scope for this Airflow pattern
repo's API-integrations automation). Not Freshdesk tickets. Not
the Tableau NPS view that sits downstream of trusted.
