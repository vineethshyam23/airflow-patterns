# Data flow: Advanced Replenishment partner export

## Run order

1. Upstream Vertex / foodgraph job materializes
   `ml_project.{foodgraph_preprocessed*}.advanced_replenishment_predicted_total_quantity_DE`
   and `articles_to_ingredients` (not in this folder).
2. Daily at 06:15 UTC, Composer starts
   `etl_advanced_replenishment_export`.
3. Quantity arm:
   - `start_predicted`
   - `export_ar_predicted_total_quantity_batch_{0..9}` in parallel
     (4h timeout each)
   - `end_predicted` (`ALL_DONE`)
4. Monthly arm (same run):
   - `monthly_on_5th` branches
   - If UTC day == 5:
     `export_ar_platform_pos_customers` →
     `export_ar_articles_to_ingredients`
   - Else: `skip_monthly_exports`
5. Final `end` (`ALL_DONE`).

## Per-batch quantity path

```
BQ SELECT (FARM_FINGERPRINT MOD 10 = batch)
  → row iterator
  → Avro binary encode (schema parsed once)
  → base64 value records
  → POST chunks of 2000 to /ingestbulk/{cc}/{schema_id}
```

Dates ship as strings. Numeric fields keep Avro long / double —
do not string-cast everything or the partner schema registry
rejects the payload.

Before the first token request, `stagger_parallel_token_request`
sleeps a jittered delay keyed on the batch label so ten workers do
not hit the IdP together.

## Monthly customer path

```
erp_asset (latest POS status) INNER JOIN pos transactions
  LEFT JOIN refined.match_result_erp_wholesale
  LEFT JOIN discovery.behavioural_segmentation_result
  → Avro → ingestbulk (customers schema)
  → articles_to_ingredients WHERE iso_code = 'DE'
  → Avro → ingestbulk (a2i schema)
```

Customers run before articles so a customer-table failure fails
fast before the lighter a2i POST. Both are full loads.

## Idempotency

- Quantity SELECT has no watermark. Every daily run reships the
  current prediction snapshot for that hash slice.
- Monthly subjects reship on the 5th only. Mid-month re-runs of the
  DAG skip them unless you temporarily force the branch.
- The sink is append-oriented. Duplicate posts are a consumer
  concern; do not treat "task success" as "exactly-once at the
  partner".
- Re-running a failed batch re-ships that hash slice only.

## Failure modes

| Failure | Effect | What to do |
|---------|--------|------------|
| IdP 403/429 on token | Retries with backoff; may fall to password grant | Check stagger + secret rotation; do not disable stagger |
| OAuth all grants exhausted | Task fails at startup or mid-chunk | Check Variables / realm / grant type |
| Transient HTTP | Linear backoff up to 10 attempts | Usually self-heals; else check body size |
| 401/403 mid-load | Token refresh + same chunk retry | If loops, secret rotation race — pause and fix Variables |
| One quantity batch fails | Other batches + monthly arm may still finish (`ALL_DONE`) | Re-run failed task ids; do not assume full coverage from green DAG |
| Segmentation join empty | Customers ship without segment fields | Confirm discovery table cutover / country filter |
| 413 / payload too large | Chunk POST fails | Lower `CHUNK_SIZE` |

## Scale notes

DE quantity is the wide feed — that is why batches are 10, not 5
like menu-gaps. Chunk size 2000 matches other Avro partners on this
bus. If a later market balloons past the 4-hour timeout, raise
`TOTAL_BATCHES` and `max_active_tasks` together, and keep the token
stagger.
