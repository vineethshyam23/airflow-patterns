# Data flow: SEO business-listing GCS ingest

## Run order

1. Vendor (or ops) drops `.json` / `.json.gz` NDJSON under `uploads/`.
2. Trigger `etl_seo_business_listing_ingest` manually (`schedule=None`).
3. `ingest_all_uploads` promotes every eligible landing object:
   - stream → `stg_to_load/uncompressed_{stem}.json`
   - copy → `archive_raw/{stem}.json(.gz)`
   - delete landing object
4. `load_seo_to_bq` truncates `staging.seo_establishments` from the
   wildcard staging prefix.
5. `dbt_seo_listings` builds / refreshes refined listing tables
   (EmptyOperator when job Variable unset).
6. `archive_all_stg_to_load` moves staging objects to
   `archive_ingested/`.

## Per-object promote path

```
uploads/{original}
  → detect compression (magic bytes)
  → stream scan (countries, min/max ts, content-md5, record-count)
  → write stg_to_load/uncompressed_{YYYYMMDD}_countries-{n}_{raw_md5[:8]}.json
  → copy archive_raw/{stem}.json(.gz) with x-goog-meta-* metadata
  → delete uploads/{original}
```

Metadata keys include `original-filename`, `landing-path`, `md5-hash`,
`content-md5`, `countries`, `country-count`, `min-ts`, `max-ts`,
`record-count`, `upload-date`, `raw-md5-short`, `paired-object`,
`compression`.

## Idempotency

- Promote always processes every `uploads/` candidate. Re-dropping the
  same vendor file creates a new stem if GCS md5 differs, or a
  colliding stem if bytes are identical — ops should avoid duplicate
  landings in one run.
- BQ load is `WRITE_TRUNCATE`. Re-running mid-chain after a partial
  promote reloads whatever is currently in `stg_to_load/`.
- Archive only runs after load + dbt succeed. A failed archive leaves
  objects in `stg_to_load/` — safe to re-trigger archive alone via CLI
  `--apply`.

## Failure modes

| Failure | Effect | What to do |
|---------|--------|------------|
| Missing GCS md5 on landing object | Promote raises for that object | Re-upload; GCS should always set md5 |
| Corrupt gzip / bad NDJSON line | That object errors; others continue then raise | Fix dump; re-drop under uploads/ |
| Wrong schema_object_bucket | Load fails after successful promote | Fix Variable / env; stg objects still in stg_to_load/ |
| dbt Cloud timeout / job fail | Archive does not run | Fix dbt; re-run from dbt task or full DAG |
| Concurrent DAG runs | Staging truncate race | Keep `max_active_runs=1` |
| Empty uploads/ | Promote no-ops; load truncates to empty | Expected on dry trigger — do not schedule blindly |

## Scale notes

Stream scan is one pass over the dump — country set and timestamps
come free while writing the uncompressed staging object. Multi-GB
gzipped dumps are fine; memory stays bounded by the line iterator.
Wall-clock is dominated by GCS stream + BQ load, not by the Python
metadata pass.

## CLI dry-run

```bash
python seo_gcs_ingest.py scan /path/to/dump.json.gz
python seo_gcs_ingest.py ingest --bucket dwh-seo-business-listing
python seo_gcs_ingest.py ingest --bucket dwh-seo-business-listing --apply
python seo_gcs_ingest.py archive --bucket dwh-seo-business-listing --apply
```

Ingest/archive without `--apply` print a JSON preview and mutate nothing.
