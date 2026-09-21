# Data Flow: Adobe Analytics app Data Feed

## Object naming

| Stage | Location | Example |
|-------|----------|---------|
| Landing hit | `gs://landingzone/adobe-rawfeed-app/` | `01-app_report_suite_YYYYMMDDHH.tsv.gz` |
| Landing lookups | same prefix | `lookups_app_report_suite_….tar.gz` (members become `{dim}_…`) |
| Landing done | `…/processed/` | original basename after successful unpack |
| Composer stage | `gs://composer-data/data/adobe-rawfeed-app/` | `01-app_report_suite_….tsv`, `{dim}_app_report_suite_….tsv` |
| Composer done | `…/processed/` | moved after trusted load |

Report suite stem is a constant (`app_report_suite` here). Production
used the Adobe app report-suite id in the filename; globs in the DAG
are built from that stem so hit and lookup paths stay aligned.

## Hourly sequence

1. **extract_tar_gz_files** — unpack lookup packs into Composer data
   prefix, move landing object to `processed/`.
2. **extract_tsv_gz_files** — gunzip hit TSV into Composer data
   prefix, move landing object to `processed/`.
3. **Hit branch** — GCS→BQ staging TRUNCATE → trusted APPEND → GCS
   move to Composer `processed/`.
4. **Lookup branches (×4)** — staging TRUNCATE → DISTINCT → trusted
   TRUNCATE (+ hashes) → GCS move. Parallel under the tar extract.
5. **Barrier** — `lookup_and_app_hit_data_end` waits for hit archive
   *and* all lookup archives.
6. **Refined APPEND** — mobile enrich SQL (no hit_id anti-join unless
   you uncomment the optional guard).

## Failure modes

| Failure | Effect | Recovery |
|---------|--------|----------|
| Landing empty | Extract tasks no-op; downstream globs may fail or load empty | Next hour when Adobe drops arrive |
| Bad hit rows | Staging allows up to 1k bad records | Inspect load errors; tighter than web on purpose |
| Lookup jagged row | Staging fails (`max_bad_records=0`) | Fix dump / redeliver |
| Worker disk full | Tar extract fails mid-upload | Clear `/tmp/gcs_extracted_app/` |
| Overlap run | Prevented by `max_active_runs=1` | — |
| Refined duplicate | Possible on redelivery (no anti-join) | Enable optional WHERE; or rely on downstream dbt |
| Partial lookup success | Barrier holds refined until all archives succeed | Re-run DAG; restore from `processed/` if needed |

## Idempotency notes

- Landing → processed is destructive (copy then delete).
- Staging hit/lookup loads are TRUNCATE for files still under the
  Composer data prefix.
- Trusted hits APPEND; lookups TRUNCATE to the latest dump.
- Refined APPEND is *unguarded* in the shipped source. Web #49 uses a
  yesterday+today `hit_id` membership check — bring that over if
  Adobe redeliveries become noisy before the dbt job owns transforms.

## Distinct from pattern 49 (web)

| Concern | Web (#49) | App (#51) |
|---------|-----------|-----------|
| Landing prefix | `adobe-rawfeed-hourly/` | `adobe-rawfeed-app/` |
| Suite stem | `web_report_suite` | `app_report_suite` |
| Lookups | 13 | 4 |
| Hit bad-row budget | 50 000 | 1 000 |
| Refined focus | Browser / referrer / IP hash | App id / device / screen / event |
| hit_id anti-join | Yes (1-day window) | Off (optional, commented) |
