# Data flow: Odoo helpdesk tickets daily event export

Schedule: `30 4 * * *` (daily 04:30 UTC). Catchup off — we want
yesterday's creates relative to the run day, not a replay of every missed
calendar day (backfills need an explicit date override).

## Stage A — Refresh refined helpdesk

`DbtCloudRunJobOperator` triggers the dbt Cloud job that rebuilds
`refined.odoo_helpdesk_ticket` from upstream Odoo-landed sources.

If dbt fails, the chain stops. That is intentional: shipping yesterday's
tickets from a stale refined table looks green and lies to support
analytics.

## Stage B — Select yesterday's creates

`get_helpdesk_tickets_send_query()` builds:

```sql
SELECT ... FROM refined.odoo_helpdesk_ticket
WHERE ticket_number IS NOT NULL
  AND DATE(create_date) = CURRENT_DATE() - 1
```

No staging today/hist tables. The refined model is the source of truth
for the outbound contract.

## Stage C — Avro bulk ingest

1. `PythonOperator` → `send_helpdesk_tickets_data(country='de', query=...)`
2. Encode each row as Avro binary (base64)
3. POST chunks of 500 to `/ingestbulk/{country}/{schema_id}`

Empty result is logged, not failed — quiet support days are real.

## Idempotency and re-runs

- Re-run same day after a partial ingest: dbt refreshes again; send
  re-selects yesterday and may re-post. Downstream ingest should be
  idempotent on ticket_number (or clear the failed ingest task and
  re-run that step only).
- Backfill an older day: change the date predicate (or template it from
  `ds`). The stock DAG does not catch up historical gaps.
- Do not skip dbt on a "quick retry" unless you are sure the refined
  table already reflects the day you intend to ship.

## Failure modes worth knowing

- OAuth 401 mid-chunk: client clears token and retries the POST once.
- Empty delta: logged, not failed — expected on holidays / quiet nights.
- dbt timeout at 300s: raise the Variable-backed timeout before blaming
  the event API.
- Refined lag (Odoo extract late): dbt succeeds on incomplete upstream;
  check the Odoo → warehouse landing jobs before paging support leads.
- API accepts chunk but returns a soft error in JSON: production source
  logged responses without failing the task. Prefer
  `raise_for_status()` (as in this sanitized client) and make soft-error
  parsing explicit if the bus uses 200 + error body.
