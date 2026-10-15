# Data flow: POS vendor store-details HMAC ingest

## Steps

1. **Sign** — `HMAC-MD5(hmac_key, "getStoreDetails" + YYYYMMDD)` using
   the server calendar day. Form field `hmac` carries the hex digest.
2. **Fetch** — POST to the vendor webservice. Non-200 or empty body
   raises before any file write.
3. **Validate header** — Compare the first semicolon-delimited row to
   `EXPECTED_HEADER` (27 columns). Mismatch raises `ValueError`;
   downstream tasks never run.
4. **Local repair + write** — Parse `;` CSV, repair each data row to 27
   columns, write comma CSV under
   `data/booq_storedetails/{YYYY-MM-DD}.csv` on the Composer data
   volume.
5. **Upload** — `GCSToGCSOperator` copies Composer → rawzone
   `vendor_storedetails/booq_storedetails_{date}.csv`.
6. **GCS repair** — Re-download, normalize every row again, upload in
   place. Guards against post-copy delimiter issues before load.
7. **Load staging** — CSV → `trusted_staging.booq_storedetails` with
   schema JSON, `WRITE_TRUNCATE`.
8. **dbt** — One Cloud job (DEV/PROD id from Variable). Run id stored
   in `etl_booq_storedetails_dbt_runids` when the operator actually ran.

## Date / path coupling

`TODAY = date.today()` is computed at module import (DAG) and again
used for HMAC material via `datetime.now()` in the fetch helper.
Production lived with that for a stable morning schedule. For
backfills you want `{{ ds }}` for paths and a vendor-supported replay
strategy for auth — the daily HMAC cannot authenticate past days.

## Failure modes

| Failure | Behaviour |
|---------|-----------|
| Bad / empty HMAC key | Fetch raises; no file written |
| API non-200 | `ValueError` on status |
| Header drift | `ValueError` before local write |
| Address comma overflow | Merged into address column; row kept |
| Short row | Padded at end; warning logged |
| GCS repair / BQ load fail | Retry once (10 min); email on failure |
| dbt fail | `get_runids_task` has no ALL_DONE bypass — end stays blocked |

## Downstream

Staging feeds dbt models (trusted view of establishment + product
flags). POS analytics and CRM enrichment jobs join that view; they are
out of scope for this pattern folder.
