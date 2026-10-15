# Architecture: SEO menu URL extraction

Composer owns load/validate and country concurrency. The extractor
module owns fetch, discovery, validation, and append. Discovery is a
pure function of HTML — easy to unit-test without BQ.

## Diagram

```mermaid
flowchart TB
  subgraph upstream [Upstream]
    SEO[(de.refined_seo_business_listing)]
    VAR["Airflow Variable seo_null_menuurls_per_country"]
  end

  subgraph phase1 [Phase 1 — load and validate]
    T1[check_src_table]
    T2[get_countries_with_null_menuurls]
    T3[check_or_create_dest_table]
    T4["MERGE source → dest"]
    T5[check_dest_table]
    T6[validate_counts]
  end

  subgraph phase2 [Phase 2 — per country]
    PLAN[plan_batches]
    B1[batch_1]
    B2[batch_2]
    BN["batch_3..N"]
  end

  subgraph worker [Inside one batch]
    NTILE["NTILE partition SELECT"]
    P1[P1 source menu_url]
    P3[HTTP fetch + discover]
    PW[Cloud Run Playwright]
    VAL[HEAD/GET validate]
    FLUSH["pandas_gbq append mini-batch"]
  end

  subgraph sink [Destination]
    DEST[(de.extracted_menu_urls)]
  end

  SEO --> T1 --> T2 --> T3 --> T4 --> T5 --> T6
  VAR --> T2
  T6 --> PLAN
  PLAN --> B1
  PLAN --> B2
  PLAN --> BN
  B1 --> NTILE --> P1
  P1 -->|miss| P3
  P3 -->|403/429/timeout| PW
  P3 --> VAL --> FLUSH --> DEST
  PW --> VAL
  BN --> T8[update_variable_null_menuurls] --> ENDN[end]
```

## Components

**menu_url_utils**  
Single normalize path: resolve relative links, drop non-http(s), strip
fragments, strip query only when the path already looks like a menu.

**menu_url_discovery**  
Merge four HTML sources. JSON-LD only contributes when the graph
mentions HoReCa `@type`s; vocabulary IRIs and media extensions are
dropped. Anchors keep the old keyword behaviour so recall does not
regress when adding SPA / data-* paths.

**menu_url_extractor**  
NTILE slices of distinct websites (skip `_extraction_complete`).
ThreadPoolExecutor (5) with a shared requests Session. Deterministic
`menu_url_id` from SHA-256 of `(website, menu_url)` — no
`MAX(id)+1` races. Playwright is a Cloud Run POST with GCP identity
token, not an in-worker Chromium.

**DAG**  
Phase 1 MERGE uses `FARM_FINGERPRINT(establishment_id|website|menu_url)`
as the merge key and leaves extraction columns alone on match.
Countries chain sequentially; batches inside a country fan out under
`max_active_tasks=5`. Batch tasks use `ALL_DONE` so a failed plan still
lets empty batches no-op.

## Why NTILE instead of LIMIT/OFFSET?

Offset pagination drifts when the source grows mid-run and forces a
rescanning window. NTILE over ordered distinct URLs gives disjoint
slices that recompute from the same SQL. Combined with
`NOT EXISTS (... _extraction_complete)` the same batch id is safe to
re-run after a worker death.
