# Data Flow: Value Creation Zone bi-monthly refresh

## Happy path

1. **Trigger** — cron `15 5 3,8 * *` (05:15 UTC on the 3rd and 8th).
2. **Phase 1 fan-out** — for each of ~16 ISO markets, CREATE OR REPLACE
   five staging tables (customer, analytical customer, article,
   assortment, transactions) plus establishments. In parallel, copy
   global MAG hist, mappings, plan snapshot, Odoo invoice lines, and
   external MAG targets into `trusted_staging.vcd_*`.
3. **Barrier** — `start_storeproc` waits until every Phase 1 edge
   completes.
4. **Working tables** — WRITE_TRUNCATE reactivation IDs and customer–
   establishment feed from discovery; CREATE OR REPLACE the TTL
   transaction union (wildcard shards + BE historical cut-off).
5. **Stored procedures** — for each eligible ISO × {prod, dev},
   `CALL trusted.get_psm_uplift_values_v2(iso, env)`.
6. **Stage** — EmptyOperator joins SP and working-table chains.
7. **Status** — `check_all_tasks` collects sibling states; notification
   task runs under `ALL_DONE` and logs success or the failed task ids.
8. **End**.

## Paths

| Path | What moves | Write mode |
|------|------------|------------|
| A Country land | trusted_wholesale → `vcd_wholesale_*_{iso}` | CREATE OR REPLACE |
| B Establishments | refined.all_establishments_ISO → `vcd_all_establishments_ISO` | CREATE OR REPLACE |
| C Global refs | refined / innovation / views / external → `vcd_*` | CREATE OR REPLACE |
| D Reactivation / dashboard feed | discovery → staging | WRITE_TRUNCATE |
| E Transaction union | staging shards → discovery TTL table | CREATE OR REPLACE + 15-day expiry |
| F PSM uplift | CALL stored proc (reads staging / discovery) | procedure-owned |

## Country quirks (Austria)

| Concern | Default markets | AT |
|---------|-----------------|-----|
| Hospitality filter on customers | `LIKE '%hospitality%'` | none |
| Assortment / ABC detail cols | selected | NULL |
| `unique_wholesale_id` | source column | synthetic from store + cust |
| Assortment join | art_var_tu ⟕ private_label | article-only distinct |

## Failure modes

| Failure | Effect | Recovery |
|---------|--------|----------|
| One country CREATE fails | Barrier never opens; no SP calls | Fix source / re-run failed task then clear barrier |
| Union fails (missing shard) | SPs blocked; staging country tables may be fresh | Re-run from `start_storeproc` after fixing shard |
| One SP fails | Other env/ISO chains may still succeed; stage still reached via ALL_DONE edges | Re-run failed `call_psm_uplift_*` only |
| Prior run still active on 8th | `max_active_runs=1` blocks second run | Wait or mark prior failed after investigation |
| Notification webhook down | Portfolio logs only; production would retry Slack | Check conn / channel Variable |

## Upstream / downstream

**Upstream (must be fresh before 3rd/8th):**
- Country wholesale trusted land (`etl_mcc_*` / equivalent)
- `etl_refined_zone_monthly` MAG hist tables
- Discovery reactivation + dashboard feeds

**Downstream:**
- PSM stored procedure outputs (and sibling GCS CSV land DAG)
- Value Creation Dashboard consumers reading `vcd_*` / uplift tables

## Distinct from sibling zone DAGs

| | 46 Food Graph zone | 48 Offer Tool zone | 62 VCD (this) |
|---|--------------------|--------------------|---------------|
| Cadence | daily | daily (Wed widen) | 3rd + 8th |
| Destination | refined_foodgraph | product GCP projects | trusted_staging.vcd_* |
| Barrier purpose | partitioned history | stage publish | PSM stored proc |
| Extra | fan-in UNION ALL | SoftCircuit weekday | TTL discovery union + SP × env |
