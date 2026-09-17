# Business case: absolute multi-channel activity scores

Customer engagement teams needed a single monthly number per
establishment that survived product sprawl. Logins on the CMS, reservation
edits, order reaction rates, inbound CRM calls, portal and mobile app
usage — each lived in a different refined table with different grain and
noise. Without a shared score, "active customer" meant whatever the
asking team measured last.

## What this DAG solved

I owned the Composer shape that turned that mess into a durable contract:

1. **Base extracts** rebuild noisy sources once (derived events with a
   daily spike filter, reservation facts, Adobe / analytics login unions).
2. **Per-channel tables** project those bases onto a common
   `(salesforce_id, country_code, as_month)` grain with rolling windows
   and relative country ranks. They APPEND so history stays queryable.
3. **Fan-in score** FULL OUTER JOINs every channel, joins external KPI
   thresholds (regular vs power, per tool), and emits a 0–3
   `activity_score` plus boolean engagement flags.
4. **MoM transitions** materialize upscore / downscore / unchanged flags
   for customer-engagement dashboards — the bit ops teams actually
   opened every morning.

Schedule is `15 7 1 * *`: first of month after upstream month-close.
`next_execution_date` month-floor is the partition key everywhere; that
is load-bearing for catchup-safe re-runs.

## Tradeoffs I accepted

Embedding ~18 BigQuery jobs in one DAG was ugly. Airflow became a SQL
orchestrator with a 3k-line file and no model tests. We still shipped it
because:

- Thresholds lived in an external table that product changed without
  code review — dbt models would have needed the same join anyway.
- Fan-in ordering mattered more than elegance. A channel that finishes
  late must not publish a partial score; Composer edges made that
  explicit.
- The score fed CRM and engagement workflows the same week. Waiting on
  a perfect dbt migration would have left another quarter of
  spreadsheet definitions.

The later `etl_activity_score_job` dbt Cloud wrapper was the right
cleanup once the contract stabilized. This pattern is the pre-migration
reference: if you inherit a similar "score everything we touch" request,
start here, then move SQL — don't invent a new grain.

## What I would not reuse blindly

- WRITE_APPEND without a delete-for-month guard on the channel tables.
  Production relied on operators re-truncating bases and careful
  schedule discipline; I would add an explicit
  `DELETE WHERE as_month = …` before each append if I rebuilt it.
- Hard-coded spike filter (`n_event_per_day < 1000`). It killed a real
  incident (bot-like menu thrash) but belongs in a threshold table next
  to the KPI cuts.
- Mixing engagement (visitor / order / reservation intensity) and
  activity (any-tool login/event) into one 0–3 without documenting the
  additive formula. The formula is intentional; the docs were late.
