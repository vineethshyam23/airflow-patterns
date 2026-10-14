# Pattern 75: SEO menu content extraction (HTML → MD → BQML)

Downstream of pattern 18's menu URL ledger. Classify each effective URL,
fetch HTML into content-addressed GCS, convert to markdown, stage text in
BigQuery, then run `AI.GENERATE_TABLE` for a page-level menu object
(title / summary / JSON sections). Dish-level itemization stays out of
scope.

Source (read-only):
- `dags/etl_dataforseo_menu_extraction.py`
- `dags/horeca_digital/dataforseo_classify_menu_url_types.py`
- `dags/horeca_digital/dataforseo_fetch_menu_html.py`
- `dags/horeca_digital/dataforseo_convert_menu_html_to_md.py`
- `dags/horeca_digital/dataforseo_stage_menu_md.py`
- `dags/horeca_digital/dataforseo_extract_menu_bqml.py`

## Files

| File | Role |
|------|------|
| `dag_menu_content_extraction.py` | Ensure tables + Variable-driven country TaskGroups + batch lanes |
| `classify_menu_url_types.py` | Extension fast-path + HEAD Content-Type classify |
| `fetch_menu_html.py` | Threaded HTML fetch, SHA-256 object names, MERGE status |
| `convert_menu_html_to_md.py` | markdownify + GCS .md + stale sibling cleanup |
| `stage_menu_md.py` | Download MD → staging MERGE + FARM_FINGERPRINT menu_id |
| `extract_menu_bqml.py` | BQML `AI.GENERATE_TABLE` → idempotent menutable MERGE |
| `BUSINESS_CASE.md` | Why control-table + pre-planned NTILE + page grain |
| `ARCHITECTURE.md` | Components + Mermaid diagram |
| `DATA_FLOW.md` | Status machine, batch lanes, failure modes |

## Quick start

```bash
python -c "import ast; ast.parse(open('classify_menu_url_types.py').read())"
python -c "import ast; ast.parse(open('fetch_menu_html.py').read())"
python -c "import ast; ast.parse(open('convert_menu_html_to_md.py').read())"
python -c "import ast; ast.parse(open('stage_menu_md.py').read())"
python -c "import ast; ast.parse(open('extract_menu_bqml.py').read())"
python -c "import ast; ast.parse(open('dag_menu_content_extraction.py').read())"
```

Needs `requests`, `markdownify`, BQ remote Gemini model
(`de.gemini_menu_extract_model`), rawzone bucket, and Airflow Variable
`seo_menu_extraction_countries` (JSON list). First run with an empty
Variable only creates tables and refreshes countries for the next parse.
This folder is a sanitized reference, not a deploy package.

## Sanitization notes

- GCP projects `hd-dwh-stream-*` → `dwh_project` / `dwh_project_dev`
- Raw buckets → `dp_rawzone` / `dp_dev_rawzone`
- Dataset `dwh_de` → `de`
- Source `dataforseo_extracted_menu_urls*` → `de.extracted_menu_urls`
- Control / staging / menu tables → `menu_extraction_control_lnd` /
  `menu_md_staging` / `extracted_menus`
- GCS prefix `dataforseo/menu_content` → `seo/menu_content`
- Model `gemini_menu_extract_model_test` → `gemini_menu_extract_model`
- Variable `dataforseo_menu_extraction_countries` →
  `seo_menu_extraction_countries`
- `md_establishment_id` / `google_places_id` → `establishment_id` /
  `places_id`
- Owner / emails → `data-platform` / `dataops@example.com`
- Package imports `horeca_digital.*` → local modules
- Inline Airflow / Google / requests / markdownify imports moved to top
- Personal names and ticket IDs removed from headers

## Category

`utilities/75-dataforseo-menu-content-extraction/`
