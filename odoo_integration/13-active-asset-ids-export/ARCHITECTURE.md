# Architecture: weekly active asset ID snapshot

One refined-sales SELECT per country, thirteen parallel Avro ingest
paths, no dbt. Query helpers keep country filters out of the DAG body;
the export module owns OAuth + Avro + chunked POST.

## Diagram

```mermaid
flowchart TB
  subgraph upstream [Upstream]
    ODOO[(Odoo ERP)]
    EXTRACT[Warehouse extract / refined sales]
    CLEAN[Weekly sale_order_line cleanup DAG]
  end

  subgraph refined [Refined sales]
    SOL[(refined_sales.odoo_sale_order_line)]
    SO[(refined_sales.odoo_sale_order)]
    RC[(refined_sales.odoo_res_country)]
    RP[(refined_sales.odoo_res_partner)]
  end

  subgraph composer [Cloud Composer]
    Q[ActiveAssetIdsQueries]
    START[start]
    ING_CZ[ingest_active_asset_ids_CZ]
    ING_DE[ingest_active_asset_ids_DE]
    ING_XX["... 11 more country tasks ..."]
    ENDN[end ALL_DONE]
  end

  subgraph sinks [Event ingest]
    OAUTH[OAuth password grant]
    BULK["POST /ingestbulk/{cc}/{active_ids_schema}"]
    MFR[Partner master-file LEFT JOIN on sale_order_line_id]
  end

  ODOO --> EXTRACT --> SOL
  EXTRACT --> SO
  EXTRACT --> RC
  EXTRACT --> RP
  CLEAN --> SOL
  SOL --> Q
  SO --> Q
  RC --> Q
  RP --> Q
  START --> ING_CZ
  START --> ING_DE
  START --> ING_XX
  ING_CZ --> ENDN
  ING_DE --> ENDN
  ING_XX --> ENDN
  Q --> ING_CZ
  Q --> ING_DE
  Q --> ING_XX
  ING_CZ --> OAUTH --> BULK
  ING_DE --> OAUTH
  ING_XX --> OAUTH
  ING_DE --> BULK
  ING_XX --> BULK
  BULK --> MFR
```

## Components

**ActiveAssetIdsQueries**  
One static SELECT: DISTINCT active line id, parent order id,
establishment UUID, snapshot date. Country filter via
`odoo_res_country.code`. No SCD columns — presence is the contract.

**send_active_asset_ids_data**  
BQ client → Avro encode → chunk 500 → bulk POST. One OAuth client per
country task (tasks run in parallel). Schema parsed once per send
(production parsed every row). HTTP errors raise; production only
logged the response body.

**DAG ordering**  
`start → {13 country tasks} → end(ALL_DONE)`. No dbt. Fan-out is the
point: markets fail independently. `ALL_DONE` closes the run even when
a subset fails — useful for ops visibility, dangerous if you only
watch DAG state.

## Why not fold into pattern 09?

Different cadence (weekly vs twice-daily), different semantics
(presence vs SCD delta), different failure cost. A flaky Sunday DE
export should not block Monday morning FR lead deltas. Separate DAGs
keep the graphs honest.
