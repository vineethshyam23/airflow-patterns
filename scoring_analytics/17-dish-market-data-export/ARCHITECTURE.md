# Architecture: Establishment market-data monthly export

Composer walks countries in series. Inside each country, five parallel
hash partitions stream refined rows to Avro bulk ingest. The DAG owns
concurrency; the export module owns OAuth + encode + POST.

## Diagram

```mermaid
flowchart TB
  subgraph upstream [Upstream warehouse]
    FG[foodgraph / SEO refine DAG]
    REF[(refined.establishment_market_data_cc)]
  end

  subgraph composer [Cloud Composer]
    START[start]
    CS_ES[start_es]
    B0[export_es_batch_0]
    B1[export_es_batch_1]
    B4["export_es_batch_2..4"]
    CE_ES[end_es]
    CS_DE[start_de]
    BDE["export_de_batch_0..4"]
    CE_DE[end_de]
    ENDN[end]
  end

  subgraph partition [Hash partition per batch]
    FP["FARM_FINGERPRINT establishment_id MOD 5"]
  end

  subgraph sinks [Event ingest]
    OAUTH[OAuth password grant]
    AVRO[Avro encode chunk 1000]
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
  CE_ES --> CS_DE
  CS_DE --> BDE --> CE_DE --> ENDN
  REF --> FP
  FP --> B0
  FP --> B1
  FP --> B4
  FP --> BDE
  B0 --> OAUTH --> AVRO --> BULK
  B1 --> OAUTH
  B4 --> OAUTH
  BDE --> OAUTH
```

## Components

**countries**  
Single active-ISO list. Export module and DAG both import it so a
market cannot be scheduled without also appearing in the query path
(and vice versa).

**market_data_export**  
Builds the `FARM_FINGERPRINT … MOD N = batch` SELECT over the full
country table. Streams the BQ iterator, Avro-encodes each row
(doubles for geo; strings elsewhere; JSON values serialized in
Python), POSTs chunks of 1000. Schema parsed once per batch. OAuth
retries transient failures with linear backoff; 401 clears the token
and retries the same payload.

**DAG ordering**  
`start → {start_cc → [batch_0..4] → end_cc}×countries → end`.

Countries are chained (`end_es → start_de → …`). Batches inside a
country fan out under `max_active_tasks=5`. Both `end_{cc}` and `end`
use `ALL_DONE` so a single failed partition does not freeze the
graph — monitor task state separately.

## Why hash partitions instead of LIMIT/OFFSET?

Offset pagination rescans and drifts under concurrent writes. A
stable fingerprint of `establishment_id` gives disjoint slices that
re-runs can recompute without a batch column on the refined table.
Cost is one hash per row in the WHERE clause — cheap relative to the
Avro + HTTP work on a ~300k-row country.

## Why not delta like scoring exports?

Patterns 04 / 16 hash-delta a narrow score or category slice. This
contract is a wide listing document. Full monthly load keeps the
partner snapshot coherent without inventing a fragile row fingerprint
across nested JSON columns.
