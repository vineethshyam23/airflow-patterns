# Data flow: SEO menu URL extraction

## Trigger

`schedule_interval=None` in the sanitized sample — production used a
manual / on-demand trigger (and experimented with quarterly cron).
Expect a 72h `dagrun_timeout` when backfilling large markets.

## Inputs

| Input | Role |
|-------|------|
| `de.refined_seo_business_listing` | Read-only source: website, optional menu_url, country, establishment_id, places_id |
| `seo_null_menuurls_per_country` | Airflow Variable `{country: null_menu_url_count}` drives TaskGroup names at parse time |
| `playwright_scraper_url` | Optional Cloud Run base URL for JS render |
| `MENU_URL_PROXY*` / `MENU_URL_PROXY_ON_403_RETRY` | Optional HTTP proxies |
| `MENU_URL_JS_HEAVY_DOMAINS` | Comma-separated hosts that skip straight to Playwright after HTTP fail |

## Phase 1 — load

1. Assert source exists; count distinct `establishment_id`.
2. Load country sizes from Variable (fail closed if empty).
3. Ensure destination schema (create or add missing columns).
4. MERGE source → dest. Incremental when `dbt_updated_at` exists and
   dest is non-empty; otherwise full. Matched rows refresh listing
   columns only — extraction flags stay put.
5. Re-count dest establishments; fail if totals diverge.
6. Per-country `COUNT(DISTINCT menu_url)`: dest must be ≥ source
   (Phase 2 may add fetched URLs later).

## Phase 2 — extract

For each country (sequential TaskGroups):

1. `plan_batches` → `num_batches = min(25, max(1, null_count))`.
2. `batch_1..25` run in parallel; indices beyond `num_batches` no-op.
3. Each batch: NTILE SELECT → process_source_record (P1–P6) → flush
   every 250 rows.

## Phase 3 — Variable refresh

`update_variable_null_menuurls` recounts `menu_url IS NULL` per country
and writes the Variable so the *next* DAG parse builds the right
TaskGroups. Trigger rule `ALL_DONE` so a partial extract still updates
sizes.

## Outputs

`de.extracted_menu_urls` — one or more rows per website with
`fetched_menu_url`, status, `playwright_used`, `_extraction_complete`.

## Failure modes worth watching

| Symptom | Likely cause |
|---------|----------------|
| Phase 2 missing at parse | Empty Variable — run Phase 3 once |
| Many `http_403` / Playwright | Bot wall; check scraper URL + identity token audience |
| Worker OOM | Raise mini-batch flush, lower thread workers, or cut `max_active_tasks` |
| Dest establishment count < source | MERGE bug or truncated load — stop before extract |
| Duplicate menu_url_id | Should not happen (SHA-256 id); check flush dedupe if you fork the code |
