# Data flow: Deepideas gap-ingredients export

## Weekly chain

1. Upstream refined / foodgraph loads land wholesale customers,
   transactions, establishments, menus, and recipe/ingredient mappings
   for the export country.
2. Weekly schedule — `etl_deepideas_gaps_ingredients_export` starts
   (`catchup=False`, `max_active_runs=1`).
3. **insert_today** — WRITE_TRUNCATE
   `staging.di_gap_ingredients_export_today` from
   `gaps_ingredients_queries.insert_today_query()`.
4. **ingest** — `send_data_query()` selects rows whose `_keyhash` is
   new or whose `_rowhash` changed vs yesterday.
   `send_gaps_ingredients_data(country, query)` Avro-encodes and POSTs
   chunks of 500. 401 → clear token, retry same payload.
5. **copy_yesterday** — WRITE_APPEND the same delta into
   `staging.di_gap_ingredients_export_yesterday`.
6. **update_yesterday** — soft-close superseded yesterday rows
   (`_valid_flag=False`, `_valid_until` end of prior day).

Idempotency: re-running export without refreshing "today" re-sends
the same delta. Refresh today first, or accept duplicate partner
events if their ingest is not exactly-once.

## Gap definition (do not silently change)

A row is a gap when the establishment menu implies an ingredient (via
recipe mapping) and last-year ingredient `revenue IS NULL` after
joining wholesale transactions through article→ingredient. Changing
that filter to "low revenue", "category rollup", or "below peer median"
turns this feed into pattern 21 or 16 territory.

## Hash contract quirk

Production `_keyhash` is `MD5(wholesale_id)` only. Multiple ingredient
rows share a keyhash; `_rowhash` carries the ingredient grain. The
send filter still works:

- new customer → keyhash missing from yesterday → all their gaps
- existing customer, new/changed ingredient row → keyhash known,
  rowhash new → included
- soft-close compares `concat(_keyhash, _rowhash)` against today

Do not "fix" keyhash to include ingredient without a coordinated
yesterday rebuild — soft-close semantics would drift.

## Failure modes

- Insert fails → nothing POSTed; yesterday snapshot unchanged.
- Ingest fails after insert → today is fresh; yesterday untouched.
  Fix OAuth / schema / network and re-run from ingest (or clear and
  restart the DAG if you need a clean today rebuild).
- Copy fails after successful POST → partner already has the delta;
  yesterday lag means the next successful run may re-send. Prefer
  exactly-once partner ingest or a manual yesterday catch-up.
- Update fails after copy → duplicate open versions on yesterday until
  soft-close succeeds; send SELECT still works but may over-include
  until hashes converge.

## Field notes

- Four Avro fields only. `relevance` and `product_main_cat_id` are
  strings in the registered schema — match the send SELECT casts.
- Active-buyer filter is the volume control. Ingredient grain is denser
  than category; widening the filter without checking partner quotas
  turns a weekly job into a multi-hour POST.
- Production Deepideas DAG also exported establishment (pattern 20)
  and gaps_category (pattern 21) in the same weekly graph. Those stay
  separate samples so each Avro contract stays reviewable.
