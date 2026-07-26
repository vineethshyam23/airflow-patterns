# Architecture: Ranked menu-gaps export to partner event bus

One dbt refresh, then sequential per-country blocks. Inside each
country, five parallel hash partitions stream BQ rows to Avro bulk
ingest. The DAG owns concurrency; the export module owns OAuth +
encode + POST.

## Diagram

```mermaid
flowchart TB
  subgraph upstream [Upstream warehouse]
    FG[(menu / product gap models)]
    DBT[dbt Cloud job menu_gaps_ranked]
    REF[(refined.menu_gaps_ranked_cc)]
  end

  subgraph composer [Cloud Composer]
    START[start]
    DBT_T[dbt_menu_gaps_ranked_refresh]
    CS_DE[start_de]
    B0[export_de_batch_0]
    B1[export_de_batch_1]
    B4["export_de_batch_2..4"]
    CE_DE[end_de]
    CS_XX["start_fr … start_pt"]
    ENDN[end]
  end

  subgraph partition [Hash partition per batch]
    FP["FARM_FINGERPRINT establishment_id-article_no MOD 5"]
  end

  subgraph sinks [Event ingest]
    OAUTH[OAuth password grant]
    BULK["POST /ingestbulk/country/schema_id"]
  end

  FG --> DBT --> REF
  START --> DBT_T
  DBT_T --> CS_DE
  CS_DE --> B0
  CS_DE --> B1
  CS_DE --> B4
  B0 --> CE_DE
  B1 --> CE_DE
  B4 --> CE_DE
  CE_DE --> CS_XX --> ENDN
  REF --> FP
  FP --> B0
  FP --> B1
  FP --> B4
  B0 --> OAUTH --> BULK
  B1 --> OAUTH
  B4 --> OAUTH
  B1 --> BULK
  B4 --> BULK
```

## Components

**menu_gaps_query**  
Simple per-country SELECT (+ optional D-1 filter). Useful for smoke
checks and ad-hoc full-country pulls. The live DAG path uses the
export module's partitioned query instead.

**menu_gaps_export**  
Builds the `FARM_FINGERPRINT … MOD N = batch` SELECT, streams the BQ
iterator, Avro-encodes each row, and POSTs chunks of 2000. Schema is
parsed once per batch. OAuth client retries transient failures with
linear backoff; 401 clears the token and retries the same payload.

**DAG ordering**  
`start → dbt → {start_cc → [batch_0..4] → end_cc}×countries → end`.

Countries are chained (`end_de → start_fr → …`). Batches inside a
country fan out under `max_active_tasks=5`, which equals
`TOTAL_BATCHES`, so one country saturates the pool and the next waits
on `end_{cc}`. That is intentional — not an accidental global
parallelism limit.

## Why hash partitions instead of LIMIT/OFFSET?

Offset pagination rescans and drifts under concurrent writes. A stable
fingerprint of the natural key gives disjoint slices that re-runs can
recompute without a batch column on the refined table. Cost is one
hash per row in the WHERE clause — cheap relative to the Avro + HTTP
work.
