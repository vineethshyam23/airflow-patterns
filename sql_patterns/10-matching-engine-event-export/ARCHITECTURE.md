# Architecture: Matching engine export to partner event bus

One staging rebuild, then parallel per-country Avro ingest. Prepare
owns the join + unpivot; the export module owns OAuth + Avro + chunked
POST. The DAG only wires order.

## Diagram

```mermaid
flowchart TB
  subgraph upstream [Upstream warehouse]
    MR[(trusted.match_result SCD)]
    CB[(refined.customer_base_establishment)]
    MCC[(trusted_mcc.amcc_*_customer_unique)]
  end

  subgraph composer [Cloud Composer]
    START[start]
    PREP[prepare_matching_staging]
    ING_DE[ingest_de]
    ING_FR[ingest_fr]
    ING_XX["ingest_{country} ..."]
    ENDN[end]
  end

  subgraph staging [Staging]
    STG[(refined.partner_matching_export)]
  end

  subgraph sinks [Event ingest]
    OAUTH[OAuth password grant]
    BULK["POST /ingestbulk/{country}/{schema_id}"]
  end

  MR --> PREP
  CB --> PREP
  MCC --> PREP
  START --> PREP
  PREP --> STG
  PREP --> ING_DE
  PREP --> ING_FR
  PREP --> ING_XX
  ING_DE --> ENDN
  ING_FR --> ENDN
  ING_XX --> ENDN
  STG --> ING_DE
  STG --> ING_FR
  STG --> ING_XX
  ING_DE --> OAUTH --> BULK
  ING_FR --> OAUTH
  ING_XX --> OAUTH
  ING_FR --> BULK
  ING_XX --> BULK
```

## Components

**matching_prepare**  
Per-country SQL joins current valid matches (`_valid_flag`, quality
`<= 150`, two request types) onto wholesale customer attrs and the
establishment product footprint. Pandas unpivots product flags into
service rows, applies inactive code offset (+700), dedups by activity
then oldest `date_from`, and `to_gbq(..., if_exists='replace')` the
staging table.

**matching_event_export**  
BQ SELECT by country → Avro encode → chunk 500 → bulk POST. One OAuth
client per ingest task (tasks run in parallel). Schema parsed once per
send. HTTP errors raise; 401 clears the token and retries with the
same payload.

**DAG ordering**  
`start → prepare → {ingest_hr, ingest_cz, ...} → end`. Fan-out after
prepare is the point: one rebuild contract, independent country
failure domains. A flaky `ua` API does not block `de` delivery for the
week.

## Why not one ingest task that loops countries?

A single long PythonOperator hides per-country runtime and turns one
market outage into a full-feed failure. Parallel tasks give retries,
clearer logs, and the option to clear/re-run one market without
rewriting staging.
