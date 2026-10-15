# Spec: menu extraction (Cloud Run + Composer + BigQuery)

Pragmatic spec. Written before the reference code in this folder, and short enough to review in one sitting. Patterns 18 and 75 stay the full DAG implementations. This spec only covers the seam between them.

## Objective

Turn SEO business listings into a page-level menu table.

- User of the output: scoring / menu-gap jobs that need a menu page, not a raw website.
- Success: an establishment with a website ends in `de.extracted_menus` or in a named failure status. Silent drops are a bug.

## Tech stack

- Cloud Run service, container from the Playwright Python image, gunicorn, one Chromium per request.
- Cloud Composer (Airflow 2). PythonOperator. No Chromium on the worker.
- BigQuery. Ledger, control, markdown staging, menutable. Remote Gemini model via `AI.GENERATE_TABLE` for the page extract (see pattern 75).
- GCS raw bucket for HTML and markdown objects (pattern 75). Not duplicated here.

## Commands

```bash
cd utilities/77-menu-extraction-process
python -c "import ast; ast.parse(open('scraper_client.py').read())"
python -c "import ast; ast.parse(open('dag_menu_extraction.py').read())"
python -c "import ast; ast.parse(open('cloud_run/app.py').read())"
# DDL is reviewed, not executed, from a laptop without a project:
test -s bq/tables.sql
```

Cloud Run image (from `cloud_run/`, when you have a project):

```bash
gcloud run deploy menu-page-renderer \
  --source . \
  --region europe-west3 \
  --no-allow-unauthenticated \
  --timeout 300 \
  --memory 2Gi
```

## Project structure

```
77-menu-extraction-process/
  PROBLEM.md            why HTTP-in-Composer fails
  SPEC.md               this file
  ARCHITECTURE.md       diagram
  DATA_FLOW.md          status machine
  bq/tables.sql         ledger, control, staging, menutable
  cloud_run/app.py      POST /fetch
  cloud_run/Dockerfile
  scraper_client.py     ID-token call from the worker
  dag_menu_extraction.py
                        phase 1 ledger MERGE + Cloud Run fallback
                        phase 2 hands URLs to the content extract
```

## Code style

One example, the call the DAG is allowed to make:

```python
html, final_url, rendered = fetch_rendered_page(
    url,
    scraper_base_url=Variable.get("playwright_scraper_url"),
)
```

Names stay boring: `menu_url_id`, `establishment_id`, `playwright_used`. No framework around three functions. SQL lives in `bq/tables.sql` or as a query string next to the task that runs it.

## Testing strategy

- `ast.parse` on the three Python modules before commit. This folder is a reference, not a deploy unit, so there is no Composer test env here.
- Cloud Run: `GET /health` returns 200. `POST /fetch` without `url` returns 400. A public HTML page returns `html`, `final_url`, and `title`.
- BigQuery: create the four tables from `tables.sql` in a scratch dataset. Re-running the script is safe (`CREATE TABLE IF NOT EXISTS`).
- DAG: a row that already has `menu_url` must not call Cloud Run (`playwright_used` stays false). A 403 from plain HTTP must call `/fetch` once.

## Boundaries

- Always: sanitize project ids, bucket names, and emails before this hits the public repo. Parameterize country. Keep page grain (one row per menu page).
- Ask first: changing `menu_id` (it is `FARM_FINGERPRINT(establishment_id || effective_url)`). Adding a dish-level table. Opening the Cloud Run service without IAM.
- Never: commit a scraper URL that is a real `*.run.app` host. Run Chromium inside the Composer task. Explode dishes in this pipeline. Copy pattern 18 or 75 task-for-task into this folder.

## Success criteria

- Architecture diagram shows Cloud Run, both DAG phases, and the four BigQuery tables.
- `bq/tables.sql` creates the ledger, control, staging, and menutable with the cluster keys the DAGs filter on.
- `scraper_client.py` sends an ID token and falls back to HTTP if the service errors.
- `dag_menu_extraction.py` MERGEs listings into the ledger and only then calls the renderer.
- README points at pattern 18 (URL task graph) and pattern 75 (HTML → markdown → BQML) instead of forking them.

## Open questions

- Dish-level itemization stays out. When that job exists it should read `de.extracted_menus`, not the ledger.
- Country TaskGroups are built from an Airflow Variable at parse time. Empty variable means "create tables and refresh the variable", same as pattern 75.
