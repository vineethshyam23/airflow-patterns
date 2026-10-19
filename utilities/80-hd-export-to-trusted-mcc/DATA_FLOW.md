# Data flow: HD export → trusted MCC materialization

## Run order

1. Upstream export-project view refresh completes before 02:00 UTC
   (not in this folder).
2. At 02:00 UTC, Composer starts `etl_hd_export_to_trusted_mcc`.
3. `start` fans out to seventeen `copy_hd_export_{cnt}` tasks in
   parallel.
4. Each copy task:
   - lists objects in `export_project.export_{cnt}`
   - for each object: `SELECT *` → `trusted_mcc.{cnt}_{name}` with
     `WRITE_TRUNCATE`
   - returns `[{"view", "error"}, …]` (possibly empty) via XCom
5. `prepare_failed_views_for_slack` (`ALL_DONE`) aggregates hard
   task failures + soft view failures and builds Slack text.
6. `send_slack_notification` posts via the Data Platform bot.
7. `end`.

## Per-country path

```
list_tables(export_project.export_{cnt})
  → for each object:
      SELECT * FROM export_{cnt}.{name}
        → WRITE_TRUNCATE dwh_project.trusted_mcc.{cnt}_{name}
  → XCom: failed views list
```

Destination table ids carry the country prefix so consumers query
one dataset (`trusted_mcc`) instead of seventeen.

## Idempotency

- Full overwrite per destination table each run.
- Re-running a single country task re-materializes that country's
  tables only.
- Soft-failed views stay absent or stale until a later run
  succeeds; they do not block sibling views in the same country.

## Failure modes

| Failure | Effect | What to do |
|---------|--------|------------|
| Cannot list source dataset | Country skipped, empty XCom | Check IAM / dataset exists in export project |
| Single view GoogleAPIError | Recorded in XCom; loop continues | Fix view / permissions; re-run country task |
| Known skip view in SKIP_FAILED_VIEWS | Omitted from Slack | Expected noise filter |
| Hard task failure (e.g. worker OOM) | Appears in DAG Failure Slack | Re-run failed task id |
| Slack conn / channel missing | Notify task fails after copies | Fix Variable / invite bot; copies already landed |
| Upstream export late | Stale truncate of yesterday's view | Prefer Dataset dependency over cron hope |

## Scale notes

Seventeen parallel query jobs can spike slot usage. If the overnight
window starts colliding with other heavy jobs, lower parallelism
with a TaskGroup pool or split markets across two DAGs — do not
serialize all countries into one task just to save slots unless the
window is already comfortable.
