# Data Flow: Keycloak SSO events land

## Object naming

| Stage | Location | Example |
|-------|----------|---------|
| Identity backup | `gs://identity-backups/` | `keycloak_events` (tar.gz blob) |
| Composer tar | `gs://composer-data/data/sso/` | `YYYY-MM-DD-kc-events-table.csv.tar.gz` |
| Composer CSV | same prefix (after unpack) | `YYYY-MM-DD-kc-events-table.csv` |
| Rawzone | `gs://rawzone/sso/events/YYYY-MM-DD/` | `events.csv` |
| Schema | `gs://rawzone/schema_json/` | `sso_events.json` |
| Staging | `{project}.trusted_staging.sso_events` | WRITE_TRUNCATE each run |
| Trusted | `{project}.trusted.sso_events` | WRITE_APPEND each run |

`YYYY-MM-DD` is **yesterday relative to DAG parse time** in production
(and in this sample). It is not `{{ ds }}`.

## Daily sequence (07:00 UTC)

1. **copy_sso_file** — GCSToGCS: identity backup object → Composer
   `data/sso/{loaddate}-kc-events-table.csv.tar.gz`.
2. **gunzip_sso_file** — Bash `tar -xzf` on the worker FUSE mount into
   the same directory (produces the CSV next to the tar).
3. **remove_gz_file** — delete the tar on the worker so `data/sso/`
   does not accumulate archives.
4. **sso_file_to_bucket** — GCSToGCS: Composer CSV → rawzone
   `sso/events/{loaddate}/events.csv`.
5. **load_sso_data** — GCSToBigQuery CSV load into staging,
   WRITE_TRUNCATE, `max_bad_records=100`, skip header.
6. **data_insert_sso** — BigQuery INSERT…SELECT into trusted with
   lineage columns and `TIMESTAMP_MILLIS(event_timestamp)`.

## Failure modes

| Failure | Effect | Recovery |
|---------|--------|----------|
| Backup object missing / late | copy_sso_file fails; no retries | Wait for identity export; clear and re-run |
| Tar extract fails | CSV never appears; remove_gz never runs | Inspect worker disk / corrupt archive; fix upstream |
| Rawzone publish fails | Staging still previous day | Re-run from sso_file_to_bucket after CSV exists |
| Jagged CSV rows | Up to 100 bad records skipped | Inspect load errors; tighten schema if systemic |
| Trusted insert fails after staging truncate | Staging holds today's file; trusted missing today's append | Fix SQL / permissions; re-run insert only if staging still good |
| Same-day re-run | Staging replaced; trusted gets a second append of the same ids | Delete-by-`_create_ts` / id, or add MERGE before next rebuild |
| Mid-day DAG re-parse | `loaddate` can flip relative to the scheduler's intended day | Prefer macros; avoid force-refresh unless intentional |

## Idempotency notes

- Staging is idempotent for a given day (TRUNCATE).
- Trusted is **not** idempotent. There is no `id` anti-join and no
  MERGE. Treat a successful trusted append as final for that run.
- The backup object name is stable (`keycloak_events`); the dated
  name appears only after the Composer copy. Upstream overwrite of
  the backup blob mid-day can change what a late re-run lands.

## Privacy

`ip_address`, `user_id`, `session_id`, and `details_json` can carry
identity and network data. Keep trusted access restricted the same way
you treat other auth/audit facts. Do not put raw SSO rows in a public
or broadly shared dataset in portfolio demos either — the sample uses
generic table names only.
