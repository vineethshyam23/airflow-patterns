# Architecture: BQ product API refined zone + AlloyDB sync

Composer rebuilds API-facing refined tables in BigQuery, then
incrementally syncs one market dashboard feed into AlloyDB for
low-latency product API reads.

## Diagram

```mermaid
flowchart TB
  subgraph vars [Airflow Variables]
    ENV["env DEV|PROD"]
    PROJ["dwh_project_id"]
    CRED["alloydb_*_creds JSON"]
  end

  subgraph upstream [Upstream refined / trusted]
    CB["customer_base_establishment"]
    SFDC["analytical_sfdc_establishment_actual"]
    AA["adobe_visit_visitor"]
    RT["analytical_rt_*"]
    ORD["analytical_order_orders_actual"]
    POS["pos_payment_transactions"]
    CRM["refined_sales odoo_* CRM"]
  end

  subgraph compose [Composer DAG etl_api_alloydb]
    START[start]
    W[api_product_website_refined]
    R[api_product_reservation_refined]
    O[api_product_order_refined]
    E[api_product_establishment_refined]
    P[api_product_pos_refined]
    D[api_dashboard_market_refined]
    CO[api_co_map]
    SYNC[load_data_to_alloydb]
  end

  subgraph bq [BigQuery]
    WEB[("refined.api_product_web")]
    RES[("refined.api_product_reservations")]
    ORDT[("refined.api_product_order")]
    EST[("refined.api_product_establishment")]
    POST[("refined.api_product_pos")]
    DASH[("refined.api_dashboard_market")]
    COM[("trusted_staging.api_co")]
  end

  subgraph alloy [AlloyDB PostgreSQL]
    ADASH[("api_refined.api_dashboard_market")]
  end

  subgraph consumers [Downstream]
    BQAPI["Product API reads from BQ api_*"]
    PGAPI["Latency-sensitive dashboard API"]
  end

  ENV --> START
  PROJ --> START
  CRED --> SYNC

  CB --> E
  SFDC --> E
  SFDC --> W
  SFDC --> R
  SFDC --> O
  AA --> W
  RT --> R
  ORD --> O
  POS --> P
  CRM --> D

  START --> W & R & O & E & P & D & CO
  W --> WEB
  R --> RES
  O --> ORDT
  E --> EST
  P --> POST
  D --> DASH
  CO --> COM

  W & R & O & E & P & D & CO --> SYNC
  DASH --> SYNC --> ADASH
  WEB & RES & ORDT & EST & POST --> BQAPI
  ADASH --> PGAPI
```

## Components

**dag_bq_alloydb_api_sync.py**  
Seven parallel BigQuery truncate-reloads, then one Python AlloyDB
sync. `max_active_runs=1`. Schedule `2 6 * * *`.

**product_api_queries.py**  
`ProductApiQueries` static builders for website, reservation, order,
establishment, POS KPI, and market dashboard SQL. Demo SFID remaps
use placeholder UUIDs.

## Design notes

**Why dual-store at all.** Most `api_*` tables are queried from
BigQuery by internal services that already pay the BQ latency tax.
The CRM activity dashboard needed Postgres-shaped indexes and
conflict-safe upserts closer to the request path — AlloyDB was already
on the VPC. Syncing every table would have doubled write cost for no
read gain.

**Why truncate on BQ, incremental on AlloyDB.** Refined API tables are
daily contracts: consumers expect a full current snapshot clustered on
establishment. AlloyDB holds an append-mostly activity feed keyed by
`unique_key`; replaying the full history every morning is wasteful when
`ON CONFLICT DO NOTHING` already gives idempotent re-runs.

**Why MD5 unique_key in SQL.** Activity rows lack a single natural key
once Voip / calendar / partner joins fan out. Hashing the identity
columns in BigQuery keeps the serving insert dumb and makes boundary-day
re-scans safe.

**Connection hygiene.** Production used a cursor as a context manager
and left the connection open. The sample returns `(conn, cursor)` and
closes both in `finally`. Creds stay in Variables — the source
`__main__` plaintext password block is gone.
