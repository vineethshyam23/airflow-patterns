# Data flow

## Phase 1, URL ledger

Source grain is one establishment (`establishment_id`, `places_id`, `country`, `website`, optional `menu_url`).

| Step | Writes | Status |
|------|--------|--------|
| MERGE listing → `extracted_menu_urls` | new `menu_url_id`, timestamps | `_extraction_complete = false` when `menu_url` is null |
| Source already has `menu_url` | `fetched_menu_url = menu_url` | `fetched_menu_url_status = source_menu_url`, `playwright_used = false` |
| HTTP finds a menu link | `fetched_menu_url` | `http_ok`, `playwright_used = false` |
| HTTP 403 / 429 / 503 / timeout, or JS-heavy host | Cloud Run `/fetch`, then the same link scan on returned HTML | `playwright_ok` or `fetch_failed`, `playwright_used = true` when the service returned HTML |
| No link after both | row kept | `no_menu_link`. Next run can retry. Do not delete the establishment |

`menu_url_id` is assigned on insert (`FARM_FINGERPRINT` of establishment + places id). Updates do not mint a new id.

## Phase 2, page extract

Reads `extracted_menu_urls` where `fetched_menu_url` is not null. Pattern 75 owns the status machine on `menu_extraction_control_lnd`:

`pending` → `classified` or `skipped_non_html` → `html_fetched` → `md_converted` → `staged` → `ml_extracted`

Failures (`classify_failed`, `fetch_failed`, `bqml_failed`) stay on the control row. `attempt_count` caps retries at 3. `extracted_menus` is MERGE on `menu_id`, so a re-extract updates the page instead of appending a second one.

`menu_id = FARM_FINGERPRINT(establishment_id || '|' || effective_url)`.

## What is not in this flow

Dish rows, prices, and menu-engineering classifications. Those read `extracted_menus` later. The POS menu-engineering API is a different service and is not part of this extract.
