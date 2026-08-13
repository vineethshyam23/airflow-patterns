# Data flow: Peer benchmarking gaps

## Compute DAG (daily)

1. Overnight refined loads land establishments, customers, articles,
   transactions for each ISO.
2. `05:45` UTC — `etl_benchmarking_gaps` starts (`catchup=False`,
   `max_active_runs=1`).
3. For each enabled country in parallel across countries (Composer
   pool permitting):
   - **Branch A:** topsellers (WRITE_TRUNCATE) → skeletons
   - **Branch B:** establishments → transactions
   - Both branches → gaps (WRITE_TRUNCATE into
     `refined.benchmarking_gaps_{ISO}`)
4. Gaps rows carry `_keyhash` / `_rowhash`, `_valid_from` /
   `_valid_until`, `_valid_flag` for downstream SCD-style consumers.

Idempotency: every stage is WRITE_TRUNCATE for that country table.
Re-running the DAG for a day replaces the snapshot; it does not
append duplicates.

Failure modes:

- Topsellers fails → skeletons and gaps for that ISO do not run;
  other countries continue.
- Establishments fails → same for the transactions/gaps side of that
  ISO.
- Gaps fails after parents succeed → intermediates remain inspectable;
  fix SQL / taxonomy, clear and re-run the country tasks.

## Export path (weekly / on demand)

Production wired this beside other Deepideas feeds. Benchmarking gaps
was sometimes left out of the enabled name list when the partner was
not ready — the modules still document the contract.

1. Insert / truncate `staging.di_benchmarking_gaps_export_today` from
   your flattened projection of refined gaps (market-specific INSERT
   lives upstream; not duplicated here in full).
2. `send_data_query()` selects rows whose `_keyhash` is new or whose
   `_rowhash` changed vs yesterday.
3. `send_benchmarking_gaps_data(country, query)` Avro-encodes and
   POSTs chunks of 500. 401 → clear token, retry same payload.
4. Append delta into yesterday (WRITE_APPEND), then soft-close
   superseded yesterday rows (`_valid_flag=False`).

Idempotency: re-running export without refreshing "today" re-sends
the same delta. Refresh today first, or accept duplicate partner
events if their ingest is not exactly-once.

## Field notes

- Revenue and price fields travel as strings on the Avro contract —
  that matches the registered partner schema, not a Python taste
  preference.
- PL/NL taxonomy columns differ inside the SQL builders; the DAG
  does not branch on ISO beyond task_id suffixes.
