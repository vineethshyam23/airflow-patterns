# Architecture: Adobe Analytics hourly Data Feed

Composer owns the graph. `rawfeed_extract` owns landing-zone list /
unpack / processed-move. BigQuery load uses stock GCS→BQ and insert-job
operators. Refined enrich is a SQL file joined at DAG parse/runtime.

## Diagram

```mermaid
flowchart TB
  subgraph landing [Landing zone]
    TAR["adobe-rawfeed-hourly/*.tar.gz"]
    TSV["adobe-rawfeed-hourly/*.tsv.gz"]
    PROC["adobe-rawfeed-hourly/processed/"]
  end

  subgraph extract [rawfeed_extract]
    LIST[list unprocessed]
    UNTAR[tar extract + rename]
    GUNZIP[gunzip tsv]
  end

  subgraph composer [Composer data prefix]
    NEW["data/analytics-rawfeed-hourly/new/"]
    DONE[".../new/processed/"]
  end

  subgraph dag [Hourly DAG]
    T1[extract_tar_gz_files]
    T2[extract_tsv_gz_files]
    LH[load_staging_hit_data]
    TH[load_hit_data APPEND]
    AH[archive_hit_data_file]
    LG[lookup fan-out x13]
    BAR[lookup_and_hit_data_end]
    RF[load_adobe_feed_data]
  end

  subgraph warehouse [BigQuery]
    STG_H[(trusted_staging.aa_feed_hit_data)]
    STG_L[(trusted_staging.aa_feed_*)]
    TR_H[(trusted.aa_hit_data)]
    TR_L[(trusted.aa_*)]
    REF[(refined.analytics_datafeed)]
  end

  TAR --> T1 --> LIST --> UNTAR --> NEW
  TSV --> T2 --> LIST --> GUNZIP --> NEW
  T1 --> PROC
  T2 --> PROC
  T1 --> LG --> BAR
  T2 --> LH --> STG_H --> TH --> TR_H --> AH --> DONE --> BAR
  LG --> STG_L --> TR_L
  BAR --> RF --> REF
  STG_H --> RF
  STG_L --> RF
```

## Components

**rawfeed_extract**  
Lists landing objects, skips `processed/` prefixes, downloads to
`/tmp/gcs_extracted/`, either extracts a tar or gunzips a TSV, uploads
into the Composer `new/` prefix with a suite-stem suffix, then
copy+delete into landing `processed/`.

**Hit branch**  
TSV glob `01-{report_suite}_*` → staging TRUNCATE (tab CSV, schema
object from rawzone) → trusted APPEND with lineage columns → move
objects under `new/processed/`.

**Lookup fan-out**  
For each of 13 dimension names: glob `{table}_{report_suite}_*` →
staging TRUNCATE with inline key/value schema → DISTINCT rewrite →
trusted TRUNCATE with `_keyhash` / `_rowhash` → archive. All branches
converge on `lookup_and_hit_data_end`.

**Refined enrich**  
`refined_hit_query.sql` LEFT JOINs staging lookups, builds visit/hit
IDs, hashes IP, decodes Adobe status codes, remaps a sanitized eVar /
prop set, and APPENDS rows whose `hit_id` is not already in refined
for `visit_start_time_gmt_dt >= CURRENT_DATE() - 1`.

## Why max_active_runs=1

Hourly unpack + TRUNCATE staging is not re-entrant. Overlapping runs
would race the same `new/` prefix and clobber staging mid-load. One
active run keeps land → stage → archive ordered per hour; missed hours
are recovered by whatever files remain unprocessed in landing.
