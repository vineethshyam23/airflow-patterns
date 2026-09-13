# Data flow: Midday POS customer-master refresh

## Daily path (happy case)

1. **13:00 Europe/Amsterdam** — timetable fires; Composer starts one run
   (`max_active_runs=1`).
2. **load_vendor_debtor / load_vendor_location** (parallel)
   - List blobs under `Vendor-Debtor` / `Vendor-DebLoc`.
   - Keep objects whose name contains today's `_%Y%m%dT` token.
   - Pick the blob with the latest `time_created`.
   - Load semicolon CSV → `trusted_staging.<table>_stg` with
     `WRITE_TRUNCATE`, header skip, quoted newlines allowed.
3. **dbt_vendor_customer** — rebuild customer models from refreshed
   staging (and whatever overnight tables did not change).
4. **dbt_pos_matching_ids → dbt_pos → dbt_pos_tableau** — matching and
   reporting packs that depend on fresh customer master.
5. **materialize_customer_base** — `SELECT *` from the refined view into
   a physical table (`WRITE_TRUNCATE`) for BI tools that extract
   tables, not views.
6. **end_task** — `ALL_DONE` marker for the run.

## Soft-skip

If the vendor has not dropped a same-day file for one table, that load
logs and returns. The sibling table and the dbt chain still run. This
matches ops reality: location sometimes lands later than debtor; failing
the whole afternoon run for a missing file was noisier than accepting
yesterday's staging until the next drop.

## Failure modes

| Failure | Behaviour | Ops action |
|---------|-----------|------------|
| GCS list / permission | Task fails → Slack callback | Fix IAM / bucket Variable |
| Schema drift (extra columns) | Load may ignore unknown values; missing required cols fail load | Align schema_definition with vendor header |
| Bad CSV / jagged rows | Load fails (`max_bad_records=0`) | Inspect blob; ask vendor for re-drop |
| dbt job timeout / fail | Chain stops; Slack on failed task | Retry from failure (`retry_from_failure`) or clear downstream |
| Overlapping run | Blocked by `max_active_runs=1` | Wait or mark prior run failed intentionally |

## Idempotency

Re-running the same afternoon after a new vendor drop re-selects the
newest blob and TRUNCATEs staging again. dbt jobs use
`reuse_existing_run` / `retry_from_failure` so a mid-chain clear does
not always burn a fresh Cloud run. Customer-base table is always
full-replaced from the view after a successful dbt chain.

## What this flow does not do

- Does not call the vendor HMAC API (pattern 35).
- Does not ingest GA4 (pattern 38).
- Does not apply Type 2 SCD hashes in Python — those stay overnight /
  dbt. Unused `get_rowhash` / `get_keyhash` helpers from the source
  afternoon file were dropped in sanitization.
