# Data flow: Adyen payment terminal integration

Schedule: `0 2 * * *` (early morning, before business hours in EU). Catchup
off — we want today's inventory, not a replay of every calendar day.

## Stage A — Extract from Adyen

1. **Merchants** — paginated `GET /merchants` (page size 100). Normalize ids,
   settlement currency, status, data center prefixes. Write
   `merchant_data.json` (JSONL).
2. **Stores** — for each merchant id, paginated
   `GET /merchants/{id}/stores`. Merchant list comes from XCom of the previous
   task. One merchant failure currently aborts the store fetch (returns empty);
   that is intentional fail-closed behavior from production — better than a
   silently incomplete store map.
3. **Terminals** — paginated `GET /terminals`. Assignment + connectivity fields
   flattened.
4. **Terminal settings** — per terminal (skip `assignment_status=inventory`),
   `GET .../terminalSettings` with retries. Whitelist top-level keys before
   storage so we do not land opaque blobs we never query. Fail the task if
   zero rows survive — empty settings usually means auth or path bugs.

Files land on the Composer worker under
`/home/airflow/gcs/data/adyen/payment_terminal/`.

## Stage B — Land and stage

For each of the four files:

1. `GCSToGCS` from the Composer bucket path into
   `adyen/payment_terminal/{entity}/{yyyy-mm-dd}/`
2. `GCSToBigQuery` NEWLINE_DELIMITED_JSON →
   `trusted_staging.adyen_payment_terminal_{entity}` with `WRITE_TRUNCATE`

Parallel fan-out from `stage_1` to `stage_2`. Staging is full replace per
entity per run.

## Stage C — dbt match models

Trigger the dbt Cloud job that builds
`trusted.int_payment_terminal_erp_serial_store_match` (and related models).
That table is the contract for write-backs:

| column | used for |
|--------|----------|
| `terminal_id` | reassign + PATCH target |
| `store_id` | reassign body |
| `to_disable_standalone_tip` | PATCH filter |

## Stage D — Terminal management API

**Reassign**  
`SELECT terminal_id, store_id` where both are non-null. For each row,
`POST /terminals/{id}/reassign` with `{inventory: false, storeId}`. Retry a
couple of times; log a CSV summary of outcomes. Any failure after retries
fails the task.

**Default settings PATCH**  
`SELECT terminal_id WHERE to_disable_standalone_tip IS TRUE`. PATCH body turns
off standalone mode and gratuities for the lite POS profile. Trigger rule is
`ALL_DONE` so a messy reassign batch does not block configuration cleanup.

## Idempotency and re-runs

- Staging truncates each night — re-running the extract the same day is fine.
- Reassign / PATCH are not naturally idempotent at the API layer, but repeating
  the same store assignment / same PATCH body is usually a no-op in practice.
  Still treat write-backs as privileged: dry-run in DEV first.
- XCom carries full merchant/terminal lists. Fine at our fleet size; if the
  merchant count grows large, write the list to GCS and pass a path instead.

## Failure modes worth knowing

- Settings GET after whitelist empty → counted as skipped_empty; if *all* are
  empty/failed, task fails.
- Adyen error bodies are logged in full (status, errorCode, invalidFields).
  That verbosity saved hours when a single field in the PATCH schema changed.
- Credentials live only in `adyen_payment_terminal_creds` Variable.
