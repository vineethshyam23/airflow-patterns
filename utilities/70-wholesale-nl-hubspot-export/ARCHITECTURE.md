# Architecture: Wholesale NL HubSpot reverse export

Composer owns the manual trigger and fan-out. The export module owns
OAuth2, BigQuery reads, chunking, and POST. Partner MCC sits in front
of HubSpot — we never call HubSpot APIs directly from Composer.

Three PythonOperators share a DAG id and have no cross-task edges.
Prospects, matched, and dedupe can succeed or fail independently; that
matches how CRM ops often re-runs one export type after fixing a
source table.

## Diagram

```mermaid
flowchart TB
  subgraph vars [Airflow Variables]
    AUTH["wholesale_nl_mcc_user / password + OAuth client"]
    URLS["prospect / matched / merge POST URLs"]
    SNAP["wholesale_nl_enrichment_snapshot"]
    ENV["env / dwh_gcp_project / discovery_dataset"]
  end

  subgraph discovery [BigQuery discovery]
    P["data_enrichment_WholesaleNL_HoReCa_prospect_SNAP"]
    M["data_enrichment_WholesaleNL_HoReCa_matched_SNAP"]
    D["data_enrichment_WholesaleNL_deduplication_SNAP"]
  end

  subgraph compose [Composer DAG etl_wholesale_nl_hubspot_export]
    T1[export_data_prospects]
    T2[export_data_matched]
    T3[export_data_deduplication]
  end

  subgraph partner [Partner MCC API]
    OAUTH["OAuth2 password grant"]
    EP1["POST HubSpot export prospects"]
    EP2["POST HubSpot export matched"]
    EP3["POST merge-requests"]
  end

  subgraph crm [HubSpot CRM]
    HS["Wholesale NL HoReCa prospects / customers"]
  end

  AUTH --> OAUTH
  SNAP --> P
  SNAP --> M
  SNAP --> D
  ENV --> T1
  ENV --> T2
  ENV --> T3
  P --> T1
  M --> T2
  D --> T3
  OAUTH --> EP1
  OAUTH --> EP2
  OAUTH --> EP3
  URLS --> T1
  URLS --> T2
  URLS --> T3
  T1 -->|"sessionid + records chunks of 5k"| EP1 --> HS
  T2 -->|"sessionid + records chunks of 5k"| EP2 --> HS
  T3 -->|"winning/losing id pairs"| EP3 --> HS
```

## Components

**wholesale_nl_hubspot_export.py**  
Outbound helpers only: `callAPI` (Bearer + 401 retry on POST), shared
enrichment SELECT, `export_data_prospects` / `export_data_matched` /
`export_data_deduplication`. Inbound land helpers live in pattern 69.

**dag_wholesale_nl_hubspot_export.py**  
`schedule=None`, `max_active_runs=1`, three parallel PythonOperators.
Dropped unused bucket / conn_id locals that production carried but
never wired into tasks.

## Design notes

In-memory full-table load before chunking is the production contract.
Streaming the BQ iterator into chunks would cut peak memory; I left
the load-then-chunk shape so the pattern matches what ran in Composer
and so failure semantics stay "all rows built, then POST". For very
large prospect tables, prefer an iterator refactor over raising
Composer worker memory.

`sessionid` is random per task run, not per chunk. The partner uses it
to correlate a push; all chunks from one task share the id. Re-raising
API errors (vs production's swallow) means a mid-table failure leaves
HubSpot with a partial session — ops should treat that as a CRM-side
cleanup before re-trigger.
