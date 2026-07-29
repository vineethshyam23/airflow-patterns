# Architecture: Independent-establishment menu-gaps export

Pure export DAG. Refined independent-gap tables are upstream; this
pipeline fans out hash partitions and streams Avro to the partner
ingest API. The DAG owns concurrency; the export module owns OAuth +
encode + POST.

## Diagram

```mermaid
flowchart TB
  subgraph upstream [Upstream warehouse]
    FG[(independent establishment + gap models)]
    REF[(refined.menu_gaps_independent_cc)]
  end

  subgraph composer [Cloud Composer]
    START[start]
    CS_ES[start_es]
    B0[export_es_batch_0]
    B1[export_es_batch_1]
    B4["export_es_batch_2..4"]
    CE_ES[end_es]
    CS_XX["start_next_cc …"]
    ENDN[end]
  end

  subgraph partition [Hash partition per batch]
    FP["FARM_FINGERPRINT establishment_id-menu_item-ingredient MOD 5"]
  end

  subgraph sinks [Event ingest]
    OAUTH[OAuth password grant]
    BULK["POST /ingestbulk/country/schema_id"]
  end

  FG --> REF
  START --> CS_ES
  CS_ES --> B0
  CS_ES --> B1
  CS_ES --> B4
  B0 --> CE_ES
  B1 --> CE_ES
  B4 --> CE_ES
  CE_ES --> CS_XX --> ENDN
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

**menu_gaps_indep_query**  
Simple per-country SELECT (+ optional D-1 filter) and the active ISO
list. Useful for smoke checks and ad-hoc full-country pulls. The live
DAG path uses the export module's partitioned query instead.

**menu_gaps_indep_export**  
Builds the `FARM_FINGERPRINT … MOD N = batch` SELECT, streams the BQ
iterator, Avro-encodes each row, and POSTs chunks of 1000. Schema is
parsed once per batch. OAuth client retries transient failures with
linear backoff; 401 clears the token and retries the same payload.

**DAG ordering**  
`start → {start_cc → [batch_0..4] → end_cc}×countries → end`.

Countries are chained. Batches inside a country fan out under
`max_active_tasks=5`, which equals `TOTAL_BATCHES`, so one country
saturates the pool and the next waits on `end_{cc}`. Same concurrency
contract as pattern 12 — intentional reuse, not copy-paste laziness.

## Why a separate schema from pattern 12?

Ranked gaps are account-linked commercial opportunities (article,
rank, revenue). Independent gaps are location + contact + menu signal
for establishments outside that book. Mixing them into one Avro type
forces either nullable commercial columns forever or a breaking
re-register when either side evolves. Two schemas, two DAGs, one
shared concurrency pattern.
