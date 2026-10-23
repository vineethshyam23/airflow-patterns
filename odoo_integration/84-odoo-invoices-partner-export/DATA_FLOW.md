# Data flow: Odoo invoices partner export

## Run order

1. **start** — empty marker.
2. **dbt_odoo_invoices_refresh** — deferrable dbt Cloud job that
   materializes / refreshes `refined.odoo_invoices`. 60-minute
   execution timeout; triggerer polls every 30s.
3. **start_fr → export_fr_batch_0..4 → end_fr** — five parallel
   Python tasks (pool-capped at 3) each query one fingerprint
   shard and POST Avro chunks.
4. **start_es → export_es_batch_0..4 → end_es** — same shape for
   ES; waits on `end_fr`.
5. **end** — `ALL_DONE` so a partial country failure still closes
   the DAG run for ops visibility.

## Query contract

Per batch:

```sql
SELECT … typed columns …
FROM `refined.odoo_invoices`
WHERE UPPER(country_code) = '{CC}'
  AND MOD(ABS(FARM_FINGERPRINT(CONCAT(
      CAST(odoo_id AS STRING), '-',
      CAST(odoo_invoice_id AS STRING), '-',
      IFNULL(product_code, '')
  ))), 5) = {batch_number}
```

No incremental date filter. Dates stay as DATE for Avro
`logicalType=date`; timestamps are formatted strings so the
registered schema does not need epoch micros.

## Idempotency

- Fingerprint shards are deterministic across runs for the same
  source keys.
- Partner ingest is append/snapshot semantics on their side —
  re-posting a batch after a partial fail can create duplicates
  unless the consumer dedupes on
  `(odoo_id, odoo_invoice_id, product_code, _valid_from)`.
- Prefer re-running only the failed `export_{cc}_batch_N` task
  rather than the whole DAG when dbt already succeeded.

## Auth sequence (per batch)

1. `stagger_parallel_token_request(slot_key="{cc}-batch{N}")` —
   random delay plus CRC32-stable offset so retries do not stay
   synchronized.
2. `client_credentials` with optional `realm_id` from Airflow
   Variables.
3. On non-throttle failure, password grant with the same client.
4. Ingest POST; on HTTP 401, clear token, re-acquire, retry payload.

## Failure modes

| Mode | Symptom | Action |
|------|---------|--------|
| Upstream refined empty for CC | Batch finishes with 0 rows | Check dbt models / country filter |
| SCHEMA_ID placeholder left in DEV | 4xx from ingest | Register sandbox schema; set Variable |
| Chunk timeout | RequestException after backoff | Lower CHUNK_SIZE or raise partner timeout |
| Worker OOM | Task killed mid-stream | Raise TOTAL_BATCHES and max_active_tasks together |
