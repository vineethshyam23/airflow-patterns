# Data flow: menu-engineering cross-project BQ bridge

## Run order

1. Upstream pattern-55 land refreshes `menu_eng_project.refined.co_menus`
   (assumed done before 22:00 UTC — not gated in this DAG).
2. At 22:00 UTC, Composer starts `etl_di_import`.
3. `import_co_menus` issues a BigQuery table copy:
   source `menu_eng_project.refined.co_menus` →
   destination `dwh_project.trusted.co_menus` with WRITE_TRUNCATE.
4. DWH consumers read the trusted snapshot on the next day.

## Path

```
menu_eng_project.refined.co_menus
  → BigQueryToBigQueryOperator (WRITE_TRUNCATE, CREATE_IF_NEEDED)
  → dwh_project.trusted.co_menus
```

No GCS intermediate. No SELECT rewrite. Schema follows the source
table; destination is replaced wholesale.

## Idempotency

- Full overwrite each successful run.
- Re-running the task re-copies from the current refined source.
- A failed mid-copy truncate can leave `trusted.co_menus` empty until
  a successful retry — treat empty destination after a red task as a
  real incident, not a "partial sync".

## Failure modes

| Failure | Effect | What to do |
|---------|--------|------------|
| Missing read IAM on menu_eng project | Task fails before copy | Grant Composer SA BigQuery Data Viewer on refined |
| Missing write on trusted | Task fails / truncate incomplete | Grant Data Editor on trusted; re-run |
| Upstream land late / empty | Trusted truncated to stale or empty | Fix land first; re-run bridge; consider ExternalTaskSensor |
| BQ slot contention | Retry once after 3 min | Bump retries if chronic; keep schedule late |
| Overlapping run without max_active_runs | Double truncate race | Keep `max_active_runs=1` |

## Scale notes

Single-table BQ-to-BQ is cheap relative to overnight POS or refined
SCD spines. If more menu-engineering refined tables need the same
DWH-facing publish, grow a small parallel list (pattern 81 style) —
do not invent a second orchestration pattern for the second table.
