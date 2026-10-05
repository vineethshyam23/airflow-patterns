# Business Case: VCD PSM uplift CSV land

Pattern 62 materializes VCD staging and *calls* the PSM uplift stored
procedure. That procedure writes CSV objects into a known GCS layout —
`PSM/{env}/{iso}/{yyyymm}/result|tmp/{file}.csv`. Finance still needs
those files back in BigQuery as typed staging tables before the
dashboard dbt models can run. I kept that land as a **sibling DAG** on
the same 3rd/8th calendar, scheduled a few hours later, rather than
bolting ~150 GCS→BQ tasks onto the already-heavy CREATE OR REPLACE
graph.

The engineering value is the fan-out contract: six file grains × ~13
markets × prod/dev, with an intentional schema split. Uplift grains use
an explicit schema JSON under `PSM/schema/`; Bundle and matched files
use autodetect because their columns drifted across markets and a shared
schema object became a weekly pager. EmptyOperator markers (`prod`,
`dev`, per-file) keep the Graph readable when one env or one grain is
late.

## What this unlocks

- Predictable GCS→BQ WRITE_TRUNCATE of PSM CSV objects into
  `trusted_staging.vcd_psm_{file}_{env}_{iso}` without hand-written
  load jobs per market.
- Dual-env land (prod + dev) in one DAG so dashboard and sandbox stay
  on the same calendar.
- Deferrable dbt Cloud job after the barrier — land can finish, poll
  moves to the triggerer, Composer workers are free.
- ALL_DONE status aggregation so a single missing `matched_psm_data`
  object for one ISO still surfaces in the run summary.

## Tradeoffs I accepted

- Parse-time month folders are fragile. Production used
  `datetime.now()` at DAG parse; I expose `vcd_psm_month` so a late
  re-run for prior YYYYMM does not require a code edit. Still not as
  clean as templating `{{ ds }}` into the object path — that would
  need the SP to write by logical date, which it does not today.
- Autodetect on Bundle / matched files accepts type drift. That is
  the correct ops tradeoff when commercial exports change column order
  faster than schema JSON PRs land; dbt models must tolerate nullable
  extras via `ignore_unknown_values`.
- Sharing `stage_1` across every file×env chain means one missing CSV
  blocks the dbt job for the whole run. Preferable to half-loaded uplift
  numbers on the dashboard.
- Production left `max_active_runs` unset. I add `max_active_runs=1` —
  overlapping 3rd/8th lands when a prior run overruns is how you get
  mixed-month staging tables.

## Not this pattern

- Pattern 62: VCD bi-monthly BQ staging + CALL SP (upstream producer).
- Pattern 25 / 35: other vendor GCS/CSV lands (SEO NDJSON, POS HMAC) —
  different auth and layout, not PSM dual-env fan-out.
- Pattern 48 / 52: Offer Tool product-project zone publish.
- The body of `get_psm_uplift_values_v2` and the Looker / BI layer.
