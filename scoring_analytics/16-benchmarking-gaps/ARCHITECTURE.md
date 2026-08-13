# Architecture: Peer benchmarking gaps

Composer fans out per-country BigQuery jobs. Two branches converge on
the final gaps table. A separate export path (optional) builds a
today snapshot, Avro-encodes the hash-delta vs yesterday, and POSTs
to the partner ingest API.

## Diagram

```mermaid
flowchart TB
  subgraph upstream [Upstream refined]
    EST[(refined.all_establishments_cc)]
    CUST[(refined.analytical_wholesale_customers_cc)]
    TXN[(refined.analytical_wholesale_transactions_cc)]
    ART[(refined.analytical_wholesale_articles_cc)]
  end

  subgraph composer [Cloud Composer dag etl_benchmarking_gaps]
    TS[benchmarking_topsellers_cc]
    SK[benchmarking_skeletons_cc]
    EG[benchmarking_establishments_cc]
    TR[benchmarking_transactions_cc]
    GAP[benchmarking_gaps_cc]
  end

  subgraph refined_out [Refined outputs]
    TSO[(refined.benchmarking_topsellers_cc)]
    SKO[(refined.benchmarking_gaps_skeletons_cc)]
    EGO[(refined.benchmarking_gaps_establishments_cc)]
    TRO[(refined.benchmarking_gaps_transactions_cc)]
    GAPO[(refined.benchmarking_gaps_cc)]
  end

  subgraph export_path [Optional weekly export]
    TODAY[(staging.di_benchmarking_gaps_export_today)]
    YEST[(staging.di_benchmarking_gaps_export_yesterday)]
    AVRO[Avro encode chunk 500]
    API["POST /ingestbulk/country/schema_id"]
  end

  EST --> TS
  CUST --> TS
  TXN --> TS
  ART --> TS
  TS --> TSO --> SK --> SKO
  EST --> EG --> EGO
  TXN --> TR --> TRO
  SKO --> GAP
  EGO --> GAP
  TRO --> GAP
  GAP --> GAPO

  GAPO -.-> TODAY
  TODAY --> AVRO
  YEST --> AVRO
  AVRO --> API
  TODAY --> YEST
```

## Components

**benchmarking_gaps_queries**  
SQL builders. Topsellers keep the top ~80% revenue concentration per
segment × article family. Skeletons nest article structs under
families for peer comparison. Establishments / transactions are
ARRAY_AGG staging shapes. Final gaps UNNEST those shapes, score
families with SAFE_DIVIDE + median-normalized percentiles, and assign
customer potential bands via APPROX_QUANTILES.

**dag_benchmarking_gaps**  
Loop over enabled ISO codes. Per country:

`topsellers → skeletons → gaps`  
`establishments → transactions → gaps`

Enabled set is a subset of the full ISO list so markets without
reliable taxonomy stay dark without deleting task code.

**benchmarking_gaps_export + delta_queries**  
Export path used when the partner feed is on. Hash-delta SELECT feeds
Avro encode + chunked POST. Yesterday soft-close UPDATE keeps SCD-ish
validity flags without rebuilding history tables in the partner.

## Why a diamond, not one query?

A single mega-SELECT that builds topsellers, nests skeletons, and
scores gaps in one shot is brutal to slot and impossible to debug
when one market's taxonomy drifts. Intermediate WRITE_TRUNCATE tables
are cheap compared to an on-call night spent reading a 600-line CTE.
Countries are independent; within a country the two branches overlap
on slots but share no soft dependency until `gaps`.
