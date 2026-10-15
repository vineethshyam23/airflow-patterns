# Architecture: menu extraction

Two DAGs, one renderer, four BigQuery tables.

Phase 1 finds the menu URL. Plain HTTP first. Cloud Run (headless Chromium) only when the page is JS-rendered or the site blocks the worker. Phase 2, pattern 75, turns that URL into a page-level menu row. This diagram is the whole process. The task-level graphs stay in patterns 18 and 75.

## Diagram

```mermaid
flowchart TB
  subgraph src [Upstream listing]
    LIST[(refined.seo_establishments)]
  end

  subgraph composer [Cloud Composer]
    subgraph phase1 [Phase 1 URL ledger DAG]
      SYNC[MERGE listing into ledger]
      HTTP[HTTP GET website]
      CALL[POST /fetch with ID token]
    end
    subgraph phase2 [Phase 2 content DAG pattern 75]
      CLS[classify URL type]
      FETCH[fetch HTML to GCS]
      MD[HTML to markdown]
      BQML[AI.GENERATE_TABLE]
    end
  end

  subgraph run [Cloud Run menu-page-renderer]
    HEALTH[GET /health]
    RENDER[POST /fetch Chromium]
  end

  subgraph bq [BigQuery dataset de]
    LEDGER[(extracted_menu_urls)]
    CTRL[(menu_extraction_control_lnd)]
    STAGE[(menu_md_staging)]
    MENU[(extracted_menus)]
  end

  LIST --> SYNC --> LEDGER
  LEDGER --> HTTP
  HTTP -->|403 429 timeout or JS-heavy host| CALL --> RENDER
  RENDER -->|html final_url title| CALL
  CALL -->|playwright_used true| LEDGER
  HTTP -->|menu link in static HTML| LEDGER
  LEDGER --> CLS --> CTRL
  CTRL --> FETCH --> MD --> STAGE --> BQML --> MENU
  BQML --> CTRL
```

## Components

**Cloud Run (`cloud_run/app.py`)**  
Private service. `GET /health`. `POST /fetch` with `{"url": "..."}`. Launches Chromium, waits for DOMContentLoaded plus a short settle, returns HTML, final URL, and title. gunicorn timeout 300s. No unauthenticated invoke. Composer calls it with a Google ID token whose audience is the service URL (`playwright_scraper_url`).

**Phase 1 DAG (`dag_menu_extraction.py`)**  
Reads `refined.seo_establishments`. MERGEs new establishments into `de.extracted_menu_urls` on `establishment_id` + `places_id`. Rows that already carry `menu_url` skip the renderer. Everyone else: HTTP, then Cloud Run on 403 / 429 / 503 / timeout, or when the host is on the JS-heavy list. `playwright_used` records which path won. Pattern 18 is the full country × NTILE task graph; this file is the contract those tasks implement.

**Phase 2**  
Pattern 75. Not re-implemented here. It MERGEs the ledger into `menu_extraction_control_lnd`, classifies HTML vs pdf/image, fetches HTML to GCS, stages markdown, and MERGEs `AI.GENERATE_TABLE` output into `extracted_menus` on `menu_id`.

**Tables (`bq/tables.sql`)**  
Clustered the way the DAGs filter: country, then establishment or `menu_url_id`. `CREATE TABLE IF NOT EXISTS` so a rerun does not drop history.

## Failure modes

| Mode | Behaviour |
|------|-----------|
| `playwright_scraper_url` unset | HTTP only. `playwright_used` stays false. Warning in the task log |
| Cloud Run 5xx or missing `html` | Same HTTP fallback. Ledger status `fetch_failed` if HTTP also fails |
| Source row already has `menu_url` | No render. Copied onto `fetched_menu_url` |
| Non-HTML URL in phase 2 | `skipped_non_html` on the control table. No GCS object |
| Empty country Variable | Tables created, variable refreshed, no country groups this parse |
