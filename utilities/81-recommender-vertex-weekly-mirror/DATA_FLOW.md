# Data flow: recommender Vertex weekly mirror

## Run order

1. Upstream REX market zone and DWH refined/trusted tables are
   assumed fresh enough for a Monday mirror (not gated in this DAG).
2. At 11:00 UTC Mondays, Composer starts `etl_recommender_vertex`.
3. `start` fans out to one InsertJob per `copy_jobs` entry in
   parallel (market potential + insights share).
4. Each copy task runs a SELECT-only query with BATCH priority into
   `{projectid}.{dataset}.{table}` using WRITE_TRUNCATE.
5. `end` waits for all copy tasks.

## Paths

### Cross-project market potential

```
rex_vertex_project.datazone_metro_pl.{source_table}
  → SELECT *
  → WRITE_TRUNCATE dwh_project.refined.pol_*_potential
```

### Intra-DWH insights share

```
dwh_project.{refined|trusted|product_spot|…}.*
  → SELECT / light join + key reshape
  → WRITE_TRUNCATE dwh_project.insights_share.{table}
```

Destination dataset is part of the job dict so the same loop can
target more than one dataset without branching operators.

## Idempotency

- Full overwrite per destination table each run.
- Re-running a single `copy_*` task re-materializes that table only.
- Failed siblings do not roll back successful truncates — treat
  partial Monday success as a real state and re-run failed task ids.

## Failure modes

| Failure | Effect | What to do |
|---------|--------|------------|
| Missing IAM on REX project | Market copy tasks fail | Grant Composer SA read on market dataset |
| Missing write on insights_share | Share copies fail | Create dataset / grant write |
| Source table renamed | Query fails at InsertJob | Update SHARE_QUERIES or copy_jobs entry |
| BATCH queue delay | Task runs late but usually succeeds | Acceptable for weekly; raise priority only if SLA slips |
| Overlapping run without max_active_runs | Double truncate race | Keep `max_active_runs=1` (already set) |

## Scale notes

Eighteen-ish parallel BATCH jobs is light compared to overnight POS
or refined SCD spines. If the share map grows past a few dozen
tables, consider a TaskGroup pool or splitting by destination —
not a second orchestration pattern. Keep the registry.
