# Data flow: SEO menu content extraction

## Bootstrap / first parse

1. Set Variable `seo_menu_extraction_countries` to `[]` (or leave unset).
2. Trigger the DAG once: creates `menu_extraction_control_lnd`,
   `menu_md_staging`, `extracted_menus`, then refreshes the Variable from
   distinct countries on `de.extracted_menu_urls`.
3. Re-parse / next run: one TaskGroup per country appears.

## Per-country run

1. **Sync** — MERGE ledger → control on `menu_url_id`. New rows
   `pending` with `attempt_count=0`. Matched rows refresh source fields
   only (pipeline status untouched).

2. **Classify** — Extension regex for pdf/jpeg/png/html. Ambiguous URLs
   get HEAD (GET fallback). HTML → `classified`; other types →
   `skipped_non_html`; contact failures → `classify_failed`.

3. **Plan batches** — Count HTML rows; `num_batches = min(25, ceil(n/2000))`.
   `CREATE OR REPLACE` work table with `NTILE(num_batches)`. XCom carries
   `{country, num_batches, row_count}`.

4. **Fetch lanes (1..25)** — Join work table for `batch_id`. Fetch HTML
   with browser UA; SHA-256 name under
   `seo/menu_content/html/{country}/{menu_url_id}/`. Status
   `html_stored` or `fetch_failed`. Slots beyond plan return immediately.

5. **Convert lanes** — Download HTML, `markdownify` ATX, upload
   `seo/menu_content/markdown/.../{hash}.md`. Status `md_converted` or
   `md_convert_failed`.

6. **Stage lanes** — Download MD (cap 500k chars), MERGE into
   `menu_md_staging`, set `menu_id` =
   `FARM_FINGERPRINT(establishment_id|effective_url)`. Status
   `md_staged` or `stage_failed`.

7. **BQML lanes** — `AI.GENERATE_TABLE` over staged markdown. Success →
   MERGE `extracted_menus` + control `ml_extracted`. Failure →
   `bqml_failed` with truncated error.

8. After the last country group: **refresh Variable**, then `end`.

## Status machine (control_lnd.menu_fetch_status)

```
pending
  → classified | skipped_non_html | classify_failed
classified / fetch_failed
  → html_stored | fetch_failed
html_stored / md_convert_failed
  → md_converted | md_convert_failed
md_converted / stage_failed
  → md_staged | stage_failed
md_staged / bqml_failed
  → ml_extracted | bqml_failed
```

`attempt_count` increments only on the failure status of the *current*
stage and resets to 0 when the row advances. Cap is 3 per stage.

## XCom

`plan_batches` publishes the batch plan. Each lane pulls
`country_{safe}.plan_batches` and skips when `batch_idx > num_batches`.

## Failure / skip summary

| Condition | Outcome |
|-----------|---------|
| No countries in Variable | Ensure + refresh only |
| Country has zero HTML rows | Plan returns `num_batches=0`; lanes noop |
| Non-HTML classified | Skipped forever for this URL type |
| Attempt cap reached | Row stops retrying that stage |
| Lane task exception | That slot fails; siblings with ALL_DONE still run |
| BQML empty status | Treated as success (`ml_extracted`) |
