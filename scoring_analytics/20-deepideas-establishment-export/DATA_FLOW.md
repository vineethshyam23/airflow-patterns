# Data flow: Establishment attribute Deepideas export

## Weekly chain

1. Upstream refined / discovery / trusted loads land wholesale
   customers, establishments, menu items, geo, digitalisation, and
   market-area stats for the export country.
2. Weekly schedule — `etl_deepideas_establishment_export` starts
   (`catchup=False`, `max_active_runs=1`).
3. **insert_today** — WRITE_TRUNCATE
   `staging.di_establishment_export_today` from
   `establishment_queries.insert_today_query()`.
4. **ingest** — `send_data_query()` selects rows whose `_keyhash` is
   new or whose `_rowhash` changed vs yesterday.
   `send_establishment_data(country, query)` Avro-encodes and POSTs
   chunks of 500. 401 → clear token, retry same payload.
5. **copy_yesterday** — WRITE_APPEND the same delta into
   `staging.di_establishment_export_yesterday`.
6. **update_yesterday** — soft-close superseded yesterday rows
   (`_valid_flag=False`, `_valid_until` end of prior day).

Idempotency: re-running export without refreshing "today" re-sends
the same delta. Refresh today first, or accept duplicate partner
events if their ingest is not exactly-once.

## Failure modes

- Insert fails → nothing POSTed; yesterday snapshot unchanged.
- Ingest fails after insert → today is fresh; yesterday untouched.
  Fix OAuth / schema / network and re-run from ingest (or clear and
  restart the DAG if you need a clean today rebuild).
- Copy fails after successful POST → partner already has the delta;
  yesterday lag means the next successful run may re-send. Prefer
  exactly-once partner ingest or a manual yesterday catch-up.
- Update fails after copy → duplicate open versions on yesterday until
  soft-close succeeds; send SELECT still works but may over-include
  until hashes converge.

## Field notes

- Density and distance fields travel as the Avro types the partner
  registered (mix of int and string). Match the send SELECT casts;
  do not normalize everything to string in Python "for cleanliness."
- Active-buyer filter is the volume control. Widening it without
  checking partner quotas is how weekly jobs turn into multi-hour
  POSTs.
- Production Deepideas DAG also exported gaps_category and
  gap_ingredients after establishment. Those remain separate backlog
  items so each Avro contract stays reviewable.
