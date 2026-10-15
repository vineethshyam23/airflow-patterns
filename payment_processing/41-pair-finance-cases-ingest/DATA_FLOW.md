# Data flow: Collections partner case ingest

## Daily path (03:00 UTC)

1. **start** — EmptyOperator.
2. **Per market TaskGroup** (AT, DE, FR, ES, IT in parallel):
   1. **extract_cases** — resolve API key; if GCS object for
      `pair-finance/{ds}/{MARKET}/cases.ndjson` exists and not full
      load, skip. Else `list_cases` with `updated_from`/`updated_to`
      = UTC day window; optionally `get_case` details; `flatten_case`;
      upload via `cases_to_ndjson`.
   2. **load_gcs** — no-op when landing == dest bucket; otherwise copy
      blob. Returns early when `record_count == 0`.
   3. **has_records** — ShortCircuit; False when extract wrote zero
      rows (`ignore_downstream_trigger_rules=False` so the market join
      still completes).
   4. **stage_bq** — GCS →
      `{dwh_project|dwh_project_dev}.trusted_staging.pair_finance_cases_raw`
      with `STAGING_SCHEMA_FIELDS`, WRITE_APPEND, autodetect off.
3. **all_markets_loaded** — `TriggerRule.ALL_DONE`.
4. **dbt_job_configured** — ShortCircuit on numeric Variable
   `pair_finance_dbt_job_id` (default empty → skip).
5. **trigger_dbt_job** — dbt Cloud run; poll interval =
   `dbt_poll_interval(3600)`.
6. **end** — ALL_DONE + 30h SLA from logical date.

## Full-load seed

Trigger with conf `{"full_load": true}` (or Variable
`pair_finance_full_load`). Omits the updated_from/updated_to filter
and ignores existing GCS objects for that run.

## Idempotency

- Re-run without deleting GCS → extract skips.
- Empty market day → no upload, stage_bq skipped, siblings continue.
- dbt unset → land still succeeds; refined refresh waits on Variable.

## Failure modes

| Symptom | Likely cause | Recovery |
|---------|--------------|----------|
| Extract skipped | Object exists | Delete GCS object + staging rows for ds/market, clear tasks |
| Extract skipped (no key) | Secret missing | Provision `collections-{market}-api-key` or Variable fallback |
| stage_bq skipped | Zero cases | Check extract log for list count |
| dbt skipped | Empty job id | Set `pair_finance_dbt_job_id` |
| SLA email on skipped tasks | Historical pattern | Confirm SLA only on `end`; ignore if Grid is green |
| case_id type errors | Autodetect used | Confirm `STAGING_SCHEMA_FIELDS` and autodetect=False |
