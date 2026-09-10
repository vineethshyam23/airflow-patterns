# Data flow: Food Graph ML propagation

## Daily path (03:30 UTC)

1. **Gold (parallel)** — four WRITE_TRUNCATE jobs from Vertex
   `foodgraph_dev_preprocessed` into DWH `trusted`:
   - `fg_ingredients_translations`
   - `fg_ingredients`
   - `fg_ingredients_synonyms`
   - `fg_menu_items_drink_classification`
2. **Stage propagation (parallel, `all_done`)** — for `dev` and
   `acc`, copy unnested gaps plus articles→ingredients, translations,
   synonyms, ingredients, and synthetic menus into `trusted.fg_*_{stage}`.
3. **On-demand ShortCircuit** — if Variable
   `foodgraph_ondemand_enabled` is true, promote selected tables from
   Vertex dev → acc (validity, LLM synthetic ingredients, parsed
   menus). Default false → branch skipped.
4. **Payment-wallet match (daily)** — copy
   `matching_engine_prod.match_result_payment_wallet` into
   `trusted_staging`, run dbt Cloud job from Variable, store run ids
   on `etl_foodgraph_dbt_runids`.

Most gold/propagation tasks have no edges between them. Composer
schedules them together at DAG start; that was intentional for
throughput, not an oversight of TaskGroups.

## Month-end path (day before last calendar day)

1. ShortCircuit `day_before_month_end` returns true only then.
2. Per ISO in `ISOCODE_LIST` (DE, FR, NL, ES, PL, HR, IT, PT):
   build `rex_menu_gaps_ranked_{ISO}` in Vertex via the ranking SQL
   (top articles × FAISS × gaps × CRM cardholder keys).
3. After **all** country loads succeed, copy a fixed column list into
   `recommender_rex_project.datazone_wholesale_fr.menu_gaps_ranked${ds_nodash}`.
4. For countries in `NON_WHOLESALE_MENU_GAPS_ACTIVE` (ES): after the
   daily `fg_gaps_unnested_dev` load, build non-wholesale gaps and
   refresh `refined.partner_rex_menu_gaps_non_wholesale_es`.

Market-data monthly branch that also lives on the production DAG is
omitted here — that extract is pattern 17.

## Idempotency

- Gold and propagation: WRITE_TRUNCATE → re-run replaces the table.
- Ranked country tables: same.
- Recommender partition: WRITE_TRUNCATE on `$ds_nodash` → safe
  re-run for that logical day.
- Non-wholesale partner refresh: TRUNCATE with `_update_ts`.
- Synthetic-ingredient APPEND path from production is not wired
  (was orphaned); re-enable only with a watermark predicate.

## Failure modes

| Symptom | Likely cause | What to do |
|---------|--------------|------------|
| Cross-project query denied | Missing IAM on Vertex / REX | Grant jobUser + dataViewer/Editor on the three projects |
| Partition write: cannot add fields | `SELECT *` after schema drift | Keep the explicit column list; cast new fields only when REX agrees |
| Ranking skipped all month | ShortCircuit calendar / timezone | Confirm Composer TZ; override callable for a manual catch-up |
| Copy ran with missing countries | Old “last ISO only” edge | Sanitized DAG waits on the full country task list |
| dbt task is a stub | Variable unset / provider missing | Set `foodgraph_match_result_dbt_job_id` and install dbt provider |
| Acc promotion never runs | Variable still false | Set `foodgraph_ondemand_enabled=true` for one run, then flip back |

## Coupling

Upstream: Vertex Food Graph preprocessing and the matching engine.
Downstream: pattern 12 (ranked gaps Avro export), pattern 14
(independent gaps export), offer-tool master data, payment-wallet
match models in dbt.
