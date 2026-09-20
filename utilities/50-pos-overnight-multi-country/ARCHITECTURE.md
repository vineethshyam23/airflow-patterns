# Architecture: Overnight multi-country POS

Composer owns the graph. Helper modules own GCS→BQ landers. dbt Cloud
owns trusted SCD merge and ticket unpack. A final BigQuery insert job
materializes the refined customer-base view as a table for BI tools
that prefer tables over views.

## Diagram

```mermaid
flowchart TB
  subgraph drop [Vendor GCS drop]
    MASTER["Vendor-Machine / Article / Debtor / DebLoc CSVs"]
    TIX["transactions_daily/{country}/{v2/}orders/tickets-*.jsonl"]
    MAP["transactions_daily/{country}/cm/Tenant_Debtor_*.jsonl"]
    PROC[".../orders/processed/"]
  end

  subgraph helpers [Python helpers]
    DR[date_range.get_date_range]
    VM[vendor_master_load]
    CT[country_transactions]
  end

  subgraph dag [Overnight DAG 07:00 Europe/Amsterdam]
    LM["_load_vendor_* fan-out"]
    LMAP["load_customer_transaction_mapping_*"]
    LT["load_date_based_csv_*"]
    CDBT["pos_transactions_{ISO} dbt"]
    MV["move_files_*"]
    ENDF[end_file_loading]
    ENDM[end_of_mapping]
    DC[dbt_vendor_customer]
    DM[pos_matching_ids]
    DP[pos_transforms]
    DT[dbt_pos_tableau]
    MAT[vendor_customer_base materialize]
  end

  subgraph warehouse [BigQuery]
    STG_M[(trusted_staging.vendor_*_stg)]
    STG_T[(trusted_staging.pos_transactions_ISO)]
    TR_MAP[(trusted.vendor_customer_transaction_mapping_ISO)]
    REF[(refined.vendor_customer_base)]
  end

  MASTER --> LM --> VM --> STG_M --> DC
  MAP --> LMAP --> CT --> TR_MAP --> ENDM --> DC
  TIX --> LT --> CT --> STG_T --> CDBT --> MV --> PROC
  MV --> ENDF
  ENDF --> DM --> DP --> DT --> MAT --> REF
  DC --> DP
```

## Components

**date_range**  
Resolves daily (yesterday) vs backfill (closed Variable range) into a
list of `YYYYMMDD` strings. Load and move callables share this so a
backfill cannot archive a different set than it loaded.

**vendor_master_load**  
Lists vendor-prefixed CSVs, TRUNCATE-loads semicolon files into
`*_stg`, and exports `_rowhash` / `_keyhash` SQL used by the trusted
SCD layer. Overnight historically took the first matching blob;
afternoon (#43) later switched to newest-by-mtime for the same prefixes.

**country_transactions**  
Per ISO: APPEND ticket JSONL as a single JSON column, TRUNCATE mapping
JSONL into trusted, then copy+delete tickets into `processed/` after
country dbt succeeds. Soft-skips missing days so one dark market does
not fail the whole run.

**DAG graph**  
Master and mapping fan into customer dbt. Ticket branches fan into
`end_file_loading`, then matching → POS → Tableau → materialize.
`max_active_runs=1` avoids overlapping backfills fighting the same
staging tables.
