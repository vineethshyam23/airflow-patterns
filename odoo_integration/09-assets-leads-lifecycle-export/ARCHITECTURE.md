# Architecture: Odoo / CRM assets + leads lifecycle export

One dbt refresh, three parallel Avro ingest paths, one pilot market.
SQL helpers keep the SCD / created-date filters out of the DAG body;
the export module owns OAuth + Avro + chunked POST.

## Diagram

```mermaid
flowchart TB
  subgraph upstream [Upstream]
    CRM[(CRM / Odoo sources)]
    EXTRACT[Warehouse extract]
    DBT[dbt Cloud job: leads + assets lifecycle]
  end

  subgraph refined [Refined warehouse]
    LEADS[(refined.odoo_leads_lifecycle)]
    ASSETS[(refined.odoo_assets_lifecycle)]
    VOUCH[(refined.odoo_voucher_code)]
  end

  subgraph composer [Cloud Composer]
    Q[LifecycleQueries]
    START[start]
    RUN[dbt_odoo_leads_assets_lifecycle_refresh]
    ING_L[ingest_odoo_lead_lifecycle_data]
    ING_A[ingest_odoo_asset_lifecycle_data]
    ING_V[ingest_odoo_voucher_code_data]
    ENDN[end]
  end

  subgraph sinks [Event ingest]
    OAUTH[OAuth password grant]
    BULK_L["POST /ingestbulk/fr/{lead_schema}"]
    BULK_A["POST /ingestbulk/fr/{asset_schema}"]
    BULK_V["POST /ingestbulk/fr/{voucher_schema}"]
  end

  CRM --> EXTRACT --> DBT
  DBT --> LEADS
  DBT --> ASSETS
  DBT --> VOUCH
  LEADS --> Q
  ASSETS --> Q
  VOUCH --> Q
  START --> RUN
  RUN --> ING_L
  RUN --> ING_A
  RUN --> ING_V
  ING_L --> ENDN
  ING_A --> ENDN
  ING_V --> ENDN
  Q --> ING_L
  Q --> ING_A
  Q --> ING_V
  ING_L --> OAUTH --> BULK_L
  ING_A --> OAUTH
  ING_V --> OAUTH
  ING_A --> BULK_A
  ING_V --> BULK_V
```

## Components

**LifecycleQueries**  
Three static SELECT builders. Leads and assets filter on SCD validity
(`_valid_flag`, `_valid_from >= CURRENT_DATE()`). Vouchers filter on
`asset_created_date >= CURRENT_DATE() - 1`. Country is uppercased in
SQL; the ingest path keeps the lowercase market code the API expects.

**send_*_data**  
BQ client → Avro encode → chunk 500 → bulk POST. One OAuth client per
send task (tasks run in parallel — sharing a module-level token across
tasks is a race). Schema parsed once per send (production parsed every
row). HTTP errors raise; production only logged the response body.

**DAG ordering**  
`start → dbt → {lead, asset, voucher} → end`. Fan-out after dbt is the
point: one refresh contract, three independent ingest failure domains.
A flaky voucher API does not block lead delivery for the day.

## Why not three DAGs?

The dbt job is the shared critical path. Three sensors on the same job
id create three places to misconfigure schedule overlap and three
places to get "dbt succeeded but my DAG didn't see it" tickets. One
DAG makes the contract obvious in the graph.
