# Data flow: SEO listing GCS ingest

## Run order

1. Vendor / ops drops one or more `.json` / `.json.gz` objects under
   `gs://seo-listings-ingest/uploads/`.
2. Operator triggers `etl_seo_listings_ingestion` (no schedule).
3. `ingest_all_uploads`:
   - list eligible uploads
   - stream-scan + write uncompressed NDJSON to `stg_to_load/`
   - server-side copy original bytes to `archive_raw/`
   - delete the uploads/ source
4. `load_seo_listings_to_bq` truncates
   `dwh_project.trusted_staging.seo_business_listings` from
   `stg_to_load/uncompressed_*.json`.
5. `dbt_seo_listings` runs the Cloud job (or no-ops when unset).
6. `archive_all_stg_to_load` moves stage objects to
   `archive_ingested/` (content-md5 dedupe; delete stage).

## Object naming

```
stem = {YYYYMMDD}_countries-{n}_{raw_md5[:8]}
stg  = stg_to_load/uncompressed_{stem}.json
raw  = archive_raw/{stem}.json.gz   # or .json if plain
arc  = archive_ingested/uncompressed_{stem}.json
```

Upload date defaults to blob `time_created` (UTC). Override with
`--upload-date` / `upload_date_override` when backfilling.

## Metadata (x-goog-meta-*)

Attached to stage and archive objects after promote:

| Key | Meaning |
|-----|---------|
| `scanned` | `true` after a successful stream scan |
| `content-md5` | md5 of decompressed NDJSON line bytes |
| `md5-hash` | GCS object md5 (base64) where available |
| `countries` / `country-count` | Distinct `address_info.country_code` |
| `min-ts` / `max-ts` | Earliest / latest `time_update` or `first_seen` |
| `record-count` | Non-empty NDJSON lines |
| `upload-date` | `YYYYMMDD` used in the stem |
| `paired-object` | Sibling path (stage ↔ raw) |
| `compression` | `gzip` or `none` |

## Idempotency

- Re-dropping the same bytes into `uploads/` creates a new stem only
  if the GCS object md5 differs; same content after a full cycle is
  skipped on archive via content-md5 index.
- Ingest does **not** skip on content-md5 today — it always processes
  every uploads/ candidate. Dedup happens at archive time.
- Staging load is truncate: the BigQuery table reflects whatever is
  in `stg_to_load/` for that run.

## Failure modes

| Failure | Effect | What to do |
|---------|--------|------------|
| Invalid NDJSON line | Ingest raises; uploads/ object may still be present | Fix dump; re-trigger |
| Missing blob `md5_hash` | That object errors; others continue then RuntimeError | Re-upload object |
| BQ load schema drift | Load fails; stage kept | Align schema_json; clear bad stage if needed |
| dbt job fails | Archive does not run; stage remains | Fix dbt; clear from dbt task |
| Half archive | Some stage objects moved, errors collected | Re-run archive; dedupe protects doubles |
| Misnamed gzip | Still detected via `\x1f\x8b` | No action — by design |

## Scale notes

Dumps are multi-GB. Single-pass stream scan+write avoids local
temp disks. Wall-clock is dominated by GCS stream + BQ load, not
by the Python aggregates. Keep `max_active_runs=1`.
