# Architecture: multi-country FBO/NBO scoring export

Three layers: SQL builders that assemble a cross-country snapshot, a Composer
DAG that truncates today / fans out ingest / maintains history, and an Avro
bulk client that talks to the external event API.

## Diagram

```mermaid
flowchart TB
  subgraph refined [Refined / Trusted warehouse]
    EST["refined.all_metro_establishments_{CC}"]
    FBO[(refined.analytical_scoring_metro_customer)]
    NBO[(refined.analytical_scoring_dish_customer)]
    WL["trusted_mcc.{iso3}_hd_customer"]
    AUTH["trusted_mcc.{iso3}_hd_cust_auth_person"]
  end

  subgraph composer [Cloud Composer]
    Q[ScoringQuery SQL builders]
    INS[insert scoring_data_export_today WRITE_TRUNCATE]
    PAUSE[pause]
    ING["ingest_scoring_data_{CC} x13"]
    HIST[append scoring_data_export_hist]
    EXP[expire superseded hist rows]
  end

  subgraph staging [trusted_staging]
    TODAY[(scoring_data_export_today)]
    HISTTBL[(scoring_data_export_hist)]
  end

  subgraph events [External event API]
    OAUTH[OAuth password grant]
    BULK["POST /ingestbulk/{country}/{schema_id}"]
  end

  EST --> Q
  FBO --> Q
  NBO --> Q
  WL --> Q
  AUTH --> Q
  Q --> INS --> TODAY
  TODAY --> PAUSE --> ING
  HISTTBL -.->|previous state for delta| ING
  ING --> OAUTH --> BULK
  ING --> HIST --> HISTTBL
  HIST --> EXP --> HISTTBL
```

## Components

**ScoringQuery**  
Static builders for per-country SELECTs, the UNION ALL insert, the hist
append delta, the expire UPDATE, and the per-country send SELECT. Key idea:
`_keyhash` identities the establishment; `_rowhash` fingerprints the scored
payload. New key or changed row → outbound event.

**send_scoring_data**  
BQ client → Avro encode → chunk 500 → bulk POST. OAuth client refreshes once
on 401 so a long DE/FR loop does not die mid-run.

**DAG ordering**  
`today` truncate → parallel country ingest (against *previous* hist) → hist
append → hist expire. That order is load-bearing. A later production revision
moved the SQL into two dbt Cloud jobs for the same reason: job 1 builds today
only; job 2 updates hist *after* ingest.

## Why hash-delta and not full reload?

Full reload to the event bus was fine at pilot volume. At a dozen markets it
burned API quota and made downstream consumers reprocess unchanged rows.
Hash compare against last month's hist cut payload size dramatically on quiet
months without inventing a CDC stack. Good enough for monthly scoring; I would
not use this for high-churn transactional feeds.
