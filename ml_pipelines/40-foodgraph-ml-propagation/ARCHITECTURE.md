# Architecture: Food Graph ML propagation

Composer DAG that pulls Vertex Food Graph preprocessed tables into
the DWH, builds gold reference tables, ranks menu gaps near month-end
for the recommender project, and refreshes payment-wallet match
results through dbt Cloud.

## Diagram

```mermaid
flowchart TB
  subgraph vertex [Vertex ML project]
    DEV[(foodgraph_dev_preprocessed)]
    ACC[(foodgraph_acc_preprocessed)]
    MATCH[(matching_engine_prod)]
  end

  subgraph airflow [Cloud Composer]
    GOLD[Gold BQ jobs x4]
    PROP[dev/acc propagation loop]
    OD{ShortCircuit on_demand}
    ME{ShortCircuit day_before_month_end}
    RANK[rex_menu_gaps_ranked per ISO]
    COPY[Fixed-column partition copy]
    PAY[match_result → dbt Cloud]
  end

  subgraph dwh [DWH project]
    T[(trusted.fg_*)]
    TS[(trusted_staging.match_result_*)]
    R[(refined.partner_rex_*)]
  end

  subgraph rex [Recommender project]
    MG[(datazone_wholesale_fr.menu_gaps_ranked$ds)]
  end

  DEV --> GOLD
  DEV --> PROP
  ACC --> PROP
  GOLD --> T
  PROP --> T
  OD -->| ACC
  ME --> RANK
  T --> RANK
  RANK --> COPY
  COPY --> MG
  PROP --> R
  MATCH --> PAY
  PAY --> TS
```

## Components

**Gold jobs**  
Four WRITE_TRUNCATE queries over Vertex `foodgraph_dev_preprocessed`
into `trusted.fg_*`. Dedup via DISTINCT + hash columns for lineage
metadata. No row-count gates — re-run is the recovery path.

**Stage propagation**  
For `dev` and `acc`, copy gaps (unnested), articles→ingredients,
translations, synonyms, ingredients, and synthetic menus. Most tasks
use `trigger_rule=all_done` so sibling failures do not stall the
rest of the fan-out.

**On-demand ShortCircuit**  
Variable `foodgraph_ondemand_enabled` (default false) gates acc
promotions (validity, LLM synthetic ingredients, parsed menus).
Production toggled a hard-coded `return False`; Variable is safer.

**Month-end ranking**  
ShortCircuit on day-before-month-end. Per ISO: score top articles by
own-brand / revenue / frequency × FAISS confidence, join unnested
gaps, attach wholesale CRM keys, WRITE_TRUNCATE country tables in
Vertex, then copy a fixed column list into a partitioned recommender
table.

**Payment-wallet match**  
Daily copy from matching engine → staging, dbt Cloud job id from
Variable, XCom/URL scrape into `etl_foodgraph_dbt_runids`.

## Why not separate DAGs?

Gold + propagation share the same Vertex IAM and failure domain.
Splitting month-end ranking into its own DAG is reasonable; we kept
it here because the ranked SQL reads trusted gaps produced earlier
the same day and ops already monitored one Food Graph schedule.
If ranking SLA diverges, extract Phase D first.

## Failure / operability notes

- Cross-project BQ needs dataset-level access on Vertex, DWH, and
  recommender projects under `bigquery_default`.
- Partition copy must list columns; wildcard `SELECT *` fails when
  source schema grows.
- Ranking ShortCircuit uses wall-clock `date.today()` in this
  reference — prefer logical date in production for backfills.
- dbt stub replaces the Cloud operator when the Variable is unset so
  the file still parses in a laptop AST check.
