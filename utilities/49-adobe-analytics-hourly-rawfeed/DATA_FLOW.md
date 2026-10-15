# Data Flow: Adobe Analytics hourly Data Feed

## Object naming

| Stage | Location | Example |
|-------|----------|---------|
| Landing hit | `gs://landingzone/adobe-rawfeed-hourly/` | `01-web_report_suite_YYYYMMDDHH.tsv.gz` |
| Landing lookups | same prefix | `lookups_web_report_suite_….tar.gz` (members become `{dim}_…`) |
| Landing done | `…/processed/` | original basename after successful unpack |
| Composer stage | `gs://composer-data/data/analytics-rawfeed-hourly/new/` | `01-web_report_suite_….tsv`, `{dim}_web_report_suite_….tsv` |
| Composer done | `…/new/processed/` | moved after trusted load |

Report suite stem is a constant (`web_report_suite` here). Production
used the Adobe report-suite id in the filename; globs in the DAG are
built from that stem so hit and lookup paths stay aligned.

## Hourly sequence

1. **extract_tar_gz_files** — unpack lookup packs into Composer `new/`,
   move landing object to `processed/`.
2. **extract_tsv_gz_files** — gunzip hit TSV into Composer `new/`,
   move landing object to `processed/`.
3. **Hit branch** — GCS→BQ staging TRUNCATE → trusted APPEND → GCS
   move to `new/processed/`.
4. **Lookup branches (×13)** — staging TRUNCATE → DISTINCT → trusted
   TRUNCATE (+ hashes) → GCS move. Parallel under the tar extract.
5. **Barrier** — `lookup_and_hit_data_end` waits for hit archive *and*
   all lookup archives.
6. **Refined APPEND** — enrich SQL with one-day `hit_id` anti-join.

## Failure modes

| Failure | Effect | Recovery |
|---------|--------|----------|
| Landing empty | Extract tasks no-op; downstream globs may fail or load empty | Next hour when Adobe drops arrive |
| Bad hit rows | Staging load allows up to 50k bad records | Inspect load errors; tighten schema if systemic |
| Lookup jagged row | Staging fails (`max_bad_records=0`) | Fix dump / redeliver; do not raise hit budget |
| Worker disk full | Tar extract fails mid-upload | Clear `/tmp/gcs_extracted/`; consider streaming unpack |
| Overlap run | Prevented by `max_active_runs=1` | — |
| Refined duplicate | Blocked by `hit_id` anti-join on last day | Wider window if Adobe redelivers older hours |
| Partial lookup success | Barrier holds refined until all archives succeed | Re-run DAG; processed landing objects will not re-unpack — restore from `processed/` if needed |

## Idempotency notes

- Landing → processed is destructive (copy then delete). Re-running
  extract after success finds nothing unless you restore objects.
- Staging hit/lookup loads are TRUNCATE for the current hour's files
  still under `new/`.
- Trusted hits APPEND; lookups TRUNCATE to the latest dump.
- Refined APPEND is guarded by yesterday+today `hit_id` membership.
  Cross-midnight redeliveries older than that window can double-insert
  unless ops widens the predicate.

## Privacy

Visitor IP is stored as `TO_HEX(MD5(ip))` in refined. Do not reverse
that design in portfolio samples. Downstream joins should use
`mcvisid` / visid high-low, not raw network addresses.
