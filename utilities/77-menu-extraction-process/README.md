# Pattern 77: Menu extraction (Cloud Run + DAGs + BigQuery)

End-to-end contract for getting a page-level menu out of a restaurant website.

Listing rows live in BigQuery. Composer finds the menu URL. When the page is JavaScript or a bot wall, a private Cloud Run service renders it with Chromium. A second DAG (pattern 75) stores HTML, markdown, and the extracted menu. Dish-level items are out of scope.

This folder is the seam. It does not replace:

- Pattern 18, `utilities/18-dataforseo-menu-url-extraction/`, the country × batch URL DAG
- Pattern 75, `utilities/75-dataforseo-menu-content-extraction/`, HTML → markdown → `AI.GENERATE_TABLE`

## Read in this order

| File | What it answers |
|------|-----------------|
| `PROBLEM.md` | Why HTTP inside Composer loses menus |
| `SPEC.md` | What we build, how we check it, what we will not do |
| `ARCHITECTURE.md` | Cloud Run, both DAG phases, four tables |
| `DATA_FLOW.md` | Ledger statuses and the phase-2 state machine |
| `bq/tables.sql` | Ledger, control, staging, menutable |
| `cloud_run/app.py` | `POST /fetch` |
| `scraper_client.py` | ID-token call and HTTP fallback |
| `dag_menu_extraction.py` | Ledger MERGE, render task, page-level MERGE |

## Quick start

```bash
python -c "import ast; ast.parse(open('scraper_client.py').read())"
python -c "import ast; ast.parse(open('dag_menu_extraction.py').read())"
python -c "import ast; ast.parse(open('cloud_run/app.py').read())"
```

`dag_menu_extraction.py` imports Airflow, so parse it. Do not import it on a laptop without the providers.

Needs Airflow Variable `playwright_scraper_url` (Cloud Run base URL, no trailing path required) and a remote model `de.gemini_menu_extract_model` for the extract task. Tables: `bq/tables.sql`.

## Sanitization notes

- Projects `hd-dwh-stream-*` → `dwh_project`
- Dataset `dwh_de` → `de`
- Listing table → `refined.seo_establishments`
- `dataforseo_extracted_menu_urls` → `de.extracted_menu_urls`
- `md_establishment_id` / `google_places_id` → `establishment_id` / `places_id`
- Scraper host left as Variable `playwright_scraper_url`. No `run.app` URL in git
- Owner / email → `data-platform` / `dataops@example.com`

## Category

`utilities/77-menu-extraction-process/`
