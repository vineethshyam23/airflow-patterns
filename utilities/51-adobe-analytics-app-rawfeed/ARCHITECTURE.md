# Architecture: Adobe Analytics app Data Feed

Composer owns the graph. `rawfeed_extract` owns landing-zone list /
unpack / processed-move under the *app* prefix. BigQuery load uses
stock GCS→BQ and insert-job operators. Refined enrich is a SQL file
joined at DAG parse/runtime — mobile projection, four lookups.

## Diagram

```mermaid
flowchart TB
  subgraph landing [Landing zone]
    TAR["adobe-rawfeed-app/*.tar.gz"]
    TSV["adobe-rawfeed-app/*.tsv.gz"]
    PROC["adobe-rawfeed-app/processed/"]
  end

  subgraph extract [rawfeed_extract]
    LIST[list unprocessed]
    UNTAR[tar extract + rename]
    GUNZIP[gunzip tsv]
  end

  subgraph composer [Composer data prefix]
    NEW["data/adobe-rawfeed-app/"]
    DONE[".../processed/"]
  end

  subgraph dag [Hourly app DAG]
    T1[extract_tar_gz_files]
    T2[extract_tsv_gz_files]
    LH[load_staging_app_hit_data]
    TH[load_app_hit_data APPEND]
    AH[archive_app_hit_data_file]
    LG[lookup fan-out x4]
    BAR[lookup_and_app_hit_data_end]
    RF[load_adobe_appfeed_data]
  end

  subgraph warehouse [BigQuery]
    STG_H[(trusted_staging.aa_appfeed_hit_data)]
    STG_L[(trusted_staging.aa_appfeed_*)]
    TR_H[(trusted.aa_app_hit_data)]
    TR_L[(trusted.aa_app_*)]
    REF[(refined.analytics_datafeed_app)]
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
Lists `adobe-rawfeed-app/` landing objects, skips `processed/`,
downloads to `/tmp/gcs_extracted_app/`, either extracts a tar or
gunzips a TSV, uploads into the Composer `data/adobe-rawfeed-app/`
prefix with a suite-stem suffix, then copy+delete into landing
`processed/`.

**Hit branch**  
TSV glob `01-{app_report_suite}_*` → staging TRUNCATE (tab CSV, app
schema object from rawzone, `max_bad_records=1000`) → trusted APPEND
with lineage columns → move objects under Composer `processed/`.

**Lookup fan-out (×4)**  
`connection_type`, `country`, `languages`, `operating_systems`:
staging TRUNCATE with inline key/value schema → DISTINCT rewrite →
trusted TRUNCATE with `_keyhash` / `_rowhash` → archive. All branches
converge on `lookup_and_app_hit_data_end`.

**Refined enrich**  
`refined_app_hit_query.sql` LEFT JOINs connection / country / OS,
projects app eVars (product, user, establishment, screen, event),
normalizes device brand + OS aggregate, computes visit first/last hit
windows, and APPENDS to `analytics_datafeed_app`. No hit_id anti-join
in the shipped source (optional guard left commented).

## Why max_active_runs=1

Same reason as #49: hourly unpack + TRUNCATE staging is not
re-entrant. Keeping a separate DAG from web means app and web can each
hold one active run without sharing a staging race.
