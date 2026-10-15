# Architecture: Value Creation Zone bi-monthly refresh

Composer owns schedule, country fan-out, the sync barrier, stored-proc
env markers, and run-status aggregation. BigQuery owns CREATE OR
REPLACE staging tables, the TTL discovery union, and the PSM uplift
procedure. Upstream trusted_wholesale / refined / MAG tables are
assumed fresh before the 3rd/8th window.

## Diagram

```mermaid
flowchart TB
  subgraph upstream [DWH upstream]
    TW[(trusted_wholesale<br/>per-country customer / article / invoice)]
    REF[(refined<br/>establishments + analytical customers)]
    MAG[(refined MAG hist + external targets)]
    MAP[(refined_innovation<br/>MCC / platform mappings)]
    DISC[(discovery<br/>reactivation + dashboard feed)]
  end

  subgraph composer [Cloud Composer]
    START[start]
    subgraph parallel [Phase 1 parallel CREATE OR REPLACE]
      CUST[vcd_wholesale_customer_ISO]
      ART[vcd_wholesale_article_ISO]
      ASSORT[vcd_wholesale_assortment_ISO]
      TXN[vcd_wholesale_transactions_ISO]
      EST[vcd_all_establishments_ISO]
      GLOB[global MAG / mapping / Odoo invoice]
    end
    BARRIER[start_storeproc<br/>EmptyOperator barrier]
    REACT[vcd_reactivation_ids]
    UNION[discovery.v_wholesale_transaction_source<br/>15-day TTL]
    CEST[vcd_customer_establishment]
    PROD[prod marker]
    DEV[dev marker]
    SP[CALL get_psm_uplift_values_v2<br/>12 ISO × prod/dev]
    STAGE[stage]
    CHECK[check_all_tasks]
    NOTIFY[slack_notification ALL_DONE]
    ENDN[end]
  end

  subgraph staging [trusted_staging]
    VCD[(vcd_* tables)]
  end

  TW --> START
  REF --> START
  MAG --> START
  MAP --> START
  START --> CUST
  START --> ART
  START --> ASSORT
  START --> TXN
  START --> EST
  START --> GLOB
  CUST --> BARRIER
  ART --> BARRIER
  ASSORT --> BARRIER
  TXN --> BARRIER
  EST --> BARRIER
  GLOB --> BARRIER
  BARRIER --> REACT --> UNION --> CEST --> STAGE
  DISC --> REACT
  DISC --> CEST
  UNION --> PROD --> SP
  UNION --> DEV --> SP
  SP --> STAGE
  STAGE --> CHECK --> NOTIFY --> ENDN
  CUST --> VCD
  UNION --> VCD
```

## Components

**Bi-monthly schedule (`15 5 3,8 * *`)**  
Two calendar anchors per month. Early-month (3rd) catches prior-month
close; mid-month (8th) refreshes after MAG / acquisition hist land.
Not a daily zone — that is the intentional cost / stability tradeoff.

**Phase 1 parallel CREATE OR REPLACE**  
Every country job and every global reference job chains
`start → job → start_storeproc`. Airflow fans them out; the barrier
does not open until the slowest CREATE finishes.

**Austria special-casing**  
AT synthesizes `unique_wholesale_id`, nulls assortment descriptors,
and skips the hospitality filter. Keeping that in the SQL builders
avoids a second DAG just for one market's ID quirks.

**TTL discovery union**  
After the barrier, country transaction shards are wild-card-unioned
into `discovery.v_wholesale_transaction_source` with a 15-day
expiration. BE is unioned with a historical cut-off. The stored proc
reads one table; operators do not inherit permanent working tables.

**Prod / dev stored-proc fan-out**  
12 markets × 2 envs call `get_psm_uplift_values_v2`. RS / UA / AT / TR
are skipped (no procedure). EmptyOperator markers (`prod`, `dev`) keep
the Graph readable when debugging one env.

**ALL_DONE status aggregation**  
`check_all_tasks` XComs sibling states; `slack_notification` runs under
`ALL_DONE` so a partial staging failure still surfaces a summary. The
portfolio stubs the webhook to a log line.

## Boundaries

| Owns | Does not own |
|------|----------------|
| Staging materialization + barrier | PSM CSV land from GCS (sibling DAG) |
| Calling uplift stored proc | Body of `get_psm_uplift_values_v2` |
| Country SQL quirks in builders | Daily Food Graph / Offer Tool zones |
| Run-status aggregation | Dashboard Looker / BI layer |

## Operability notes

- `max_active_runs=1` — do not let a slow 3rd overrun collide with the 8th.
- Slot pressure is the failure mode on DE/FR/PL CREATE OR REPLACE nights.
- If one country CREATE fails, the barrier never opens and no SP runs —
  preferred over half-stale uplift numbers on the dashboard.
