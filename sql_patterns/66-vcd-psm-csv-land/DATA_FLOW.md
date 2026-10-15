# Data Flow: VCD PSM uplift CSV land

## Happy path

1. **Trigger** — cron `15 10 3,8 * *` (10:15 UTC on the 3rd and 8th),
   after pattern 62's SP window.
2. **Env split** — `start` fans into `dev` and `prod` EmptyOperators.
3. **Per-file markers** — each env opens six file markers
   (`Uplift_per_*`, `Bundle*`, `matched_psm_data`).
4. **Land** — for each file × ISO × env (skip `global` on matched),
   WRITE_TRUNCATE CSV from
   `PSM/{env}/{iso}/{vcd_psm_month}/{result|tmp}/{file}.csv` into
   `trusted_staging.vcd_psm_{file}_{env}_{iso}`.
5. **Barrier** — every load chain joins `stage_1`.
6. **dbt** — deferrable Cloud job `vcd_psm` transforms staging into
   dashboard / trusted_source models.
7. **Status** — `stage_2` → `check_all_tasks` → ALL_DONE notification
   → `end`.

## Paths

| Path | What moves | Write mode |
|------|------------|------------|
| A Uplift grains | GCS `result/Uplift_per_*.csv` → staging | WRITE_TRUNCATE + schema JSON |
| B Bundle grains | GCS `result/Bundle*.csv` → staging | WRITE_TRUNCATE + autodetect |
| C Matched PSM | GCS `tmp/matched_psm_data.csv` → staging | WRITE_TRUNCATE + autodetect; no `global` |
| D dbt | staging → trusted / dashboard models | dbt-owned |

## Object layout

```
gs://{vcd_psm_gcs_bucket}/
  PSM/
    schema/
      Uplift_per_quarter.json
      Uplift_per_month.json
      Uplift_per_fiscal_year.json
    {dev|prod}/
      {ES|IT|PL|...|global}/
        {YYYYMM}/
          result/
            Uplift_per_quarter.csv
            Uplift_per_month.csv
            Uplift_per_fiscal_year.csv
            BundleQuarter.csv
            BundleFiscal.csv
          tmp/
            matched_psm_data.csv
```

## Optional MERGE (not in live DAG)

`vcdb_psm_merge.sql` shows the country_code + psm_month + month/year/
Quarter natural key that an inline BQ MERGE would use. Production
commented that step out once dbt owned the trusted_source hop. Keep
the SQL as the contract if you re-enable a Composer-side merge.

## Failure modes

| Failure | Effect | Recovery |
|---------|--------|----------|
| One ISO CSV missing | That load fails; `stage_1` may still open via ALL_DONE edges on other chains — dbt then runs on partial land | Prefer fail-fast pools; re-drop missing object from SP / re-run failed load then dbt |
| Schema JSON drift on uplift grain | Load fails with schema mismatch | Fix `PSM/schema/{file}.json` or temporarily move file into autodetect set |
| Autodetect type surprise | dbt model breaks on unexpected type | Pin schema JSON for that grain; backfill staging |
| dbt job timeout / fail | Staging fresh; dashboard stale | `retry_from_failure` / re-run `vcd_psm` only |
| Prior run still active on 8th | `max_active_runs=1` blocks second run | Wait or mark prior failed after investigation |
| Wrong `vcd_psm_month` | Lands empty / wrong partition | Set Variable, clear tasks, re-run |

## Upstream / downstream

**Upstream (must exist before 10:15):**
- Pattern 62 SP writes under `PSM/{env}/{iso}/{YYYYMM}/`
- Schema JSON objects for uplift grains

**Downstream:**
- dbt VCD PSM models / Value Creation Dashboard
- Optional MERGE consumers if Composer-side trusted hop is restored

## Distinct from sibling VCD / land DAGs

| | 62 VCD SP | 25 SEO land | 66 PSM CSV (this) |
|---|-----------|-------------|-------------------|
| Cadence | 3rd + 8th 05:15 | vendor schedule | 3rd + 8th 10:15 |
| Source | DWH tables | vendor NDJSON | SP-written GCS CSV |
| Fan-out | country × SQL | path / archive | file × country × env |
| Next hop | writes CSV | staging → archive | dbt Cloud job |
