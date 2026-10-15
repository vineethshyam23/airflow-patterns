# Pattern 18: SEO business-listing menu URL extraction

Turn refined SEO / DataForSEO establishment listings into validated
menu page URLs — without an LLM. HTML heuristics (anchors, JSON-LD,
SPA hydration blobs, data-* attrs) plus optional Cloud Run Playwright
for bot-blocked or JS-heavy sites.

Source (read-only):
- `dags/etl_dataforseo_menu_url_extractor.py`
- `dags/horeca_digital/dataforseo_gbq_menu_url_extractor.py`
- `dags/horeca_digital/dataforseo_menu_url_discovery.py`
- `dags/horeca_digital/dataforseo_menu_url_utils.py`

## Files

| File | Role |
|------|------|
| `menu_url_utils.py` | Relative → absolute URL normalize; menu-path query strip |
| `menu_url_discovery.py` | Multi-source HTML discovery (no network) |
| `menu_url_extractor.py` | BQ NTILE partitions, HTTP/Playwright fetch, P1–P6 rules |
| `dag_menu_url_extractor.py` | Composer: MERGE load + per-country parallel batches |
| `BUSINESS_CASE.md` | Why heuristic crawl beat LLM classification |
| `ARCHITECTURE.md` | Components + Mermaid diagram |
| `DATA_FLOW.md` | Schedule, Variable-driven countries, failure modes |

## Quick start

```bash
python -c "import ast; ast.parse(open('menu_url_utils.py').read())"
python -c "import ast; ast.parse(open('menu_url_discovery.py').read())"
python -c "import ast; ast.parse(open('menu_url_extractor.py').read())"
python -c "import ast; ast.parse(open('dag_menu_url_extractor.py').read())"
```

Needs `selectolax` for anchor / data-* parsing, BQ tables, and
(optionally) `playwright_scraper_url` for JS renders. This folder is
a sanitized reference, not a deploy package.

## Sanitization notes

- GCP projects `hd-dwh-stream-*` → `dwh_project` / `dwh_project_dev`
- Tables `dwh_de.refined_dataforseo_business_listing` /
  `dataforseo_extracted_menu_urls` → `de.refined_seo_business_listing` /
  `de.extracted_menu_urls`
- `md_establishment_id` / `google_places_id` → `establishment_id` /
  `places_id`
- Airflow Variable `dataforseo_null_menuurls_per_country` →
  `seo_null_menuurls_per_country`
- Owner / notification emails → `data-platform` / `dataops@example.com`
- User-Agent host generalized; package imports → local modules
- Dead commented Playwright-local code dropped; Cloud Run scraper kept

## Category

`utilities/18-dataforseo-menu-url-extraction/`
