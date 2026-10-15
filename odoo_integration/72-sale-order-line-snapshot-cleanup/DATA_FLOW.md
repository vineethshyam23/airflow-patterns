# Data flow: sale_order_line snapshot soft-close

## Weekly path

1. Sunday 02:00 UTC: Composer queues the cleanup DAG.
2. `extract_odoo_ids` opens Odoo Postgres in-task, runs
   `SELECT id FROM sale_order_line ORDER BY id`, writes NDJSON locally,
   uploads to `gs://{rawzone}/odoo_cleanup/sale_order_line_ids/{{ ds }}/...`,
   and XCom-pushes the count only.
3. `load_odoo_ids_to_bq` WRITE_TRUNCATEs
   `ops.temp_odoo_sale_order_line_ids` from that object (autodetect).
4. `identify_orphans` LEFT JOINs open snapshot rows to the temp IDs,
   logs samples, XCom-pushes orphan count + first 100 IDs.
5. `update_orphan_records` UPDATE sets `dbt_valid_to = CURRENT_TIMESTAMP()`
   for the same orphan set (still open only).
6. `get_stats_and_log` compares Odoo temp count vs active snapshot,
   reports this-run closes and sync band.
7. `cleanup_temp_table` drops the temp ID table.

## Grain

One Odoo `sale_order_line.id` = one NDJSON line = one temp-table row.
Snapshot grain is the dbt snapshot row keyed by that id (plus validity
columns). Soft-close does not remove historical versions.

## Idempotency

Re-running the same Sunday interval reloads the current Odoo ID set
(`WRITE_TRUNCATE`) and only updates rows that are still open and still
missing. Already-closed orphans are left alone. There is no rollback
task — mistaken closes are a warehouse fix, not a DAG toggle.

## Sync bands (log only)

| Abs(odoo − active snapshot) | Assessment |
|-----------------------------|------------|
| < 100 | IN SYNC |
| 100–1000 | MINOR — often timing vs daily land |
| > 1000 | SIGNIFICANT — check before trusting partner ID exports |

## Operability notes

- Keep `max_active_runs=1`. Parallel runs race on the same UPDATE.
- Coordinate with weekly active-ID exports (pattern 13 family) so they
  read a closed open-set after this job.
- Identify currently materializes the full orphan id list in Pandas for
  logging. If orphan volume spikes, switch to `COUNT(*)` + `LIMIT`
  sample (noted as a follow-up in production too).
- No `active` filter on the Odoo extract — inactive lines still count
  as present; only hard-missing ids soft-close.
