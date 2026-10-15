# Data Flow: Sales data mart two-phase copy

## Happy path

1. **Trigger** — cron `5 8 * * *` (08:05 UTC daily).
2. **start** — EmptyOperator boundary.
3. **Phase 1 (parallel)** — for each entry in `VIEW_CATALOG`, run
   `SELECT * FROM {project}.{dataset}.{view}` →
   `{project}.refined_sales.{view}` with WRITE_TRUNCATE.
4. **pause** — barrier after all view tasks finish (or soft-fail rules
   allow the phase to complete).
5. **Phase 2 (parallel)** — for each entry in `TABLE_CATALOG`, native
   BigQuery-to-BigQuery copy into `refined_sales` (EU, WRITE_TRUNCATE).
6. **end** — EmptyOperator boundary.

## Paths

| Path | What moves | Operator | Write mode |
|------|------------|----------|------------|
| A View materialize | trusted_views.a_* → refined_sales | InsertJob SELECT * | WRITE_TRUNCATE |
| B Table copy | refined / trusted_odoo → refined_sales | BigQueryToBigQuery | WRITE_TRUNCATE |

## Live catalog (post-CRM freeze)

**Views (phase 1)**  
subscription history + subscriptions, Hydra establishments, catalog
price schemes / countries / products / merchants, Odoo WSL invoice lines.

**Tables (phase 2)**  
analytical CRM / Reservation Tool / Order establishment actuals,
Odoo ERP timesheets + invoice lines (timestamped), Odoo WSL customers.

Frozen CRM objects from the original ~57-object catalog stay in the
mart dataset as last-known copies and are not refreshed by this DAG.

## Failure modes

| Failure | Effect | Recovery |
|---------|--------|----------|
| Single view InsertJob fails | Sibling views may still land; pause proceeds per trigger rules; that mart table stays on prior day | Fix view / slots; clear failed task or full re-run |
| Entire phase 1 fails | Phase 2 blocked by chain | Fix sources; re-run from start |
| Single table copy fails | Other tables refresh; that object stale | Clear failed + re-run table task |
| DEV still pointed at prod project | Would overwrite prod mart (historical bug) | Sanitized code uses ENV project for both phases |
| Overnight land late | Mart copies yesterday's trusted/refined | Accept for Sales BI, or add ExternalTaskSensor |
| Prior run still active at 08:05 | `max_active_runs=1` blocks | Wait or mark failed after investigation |

## Upstream / downstream

**Upstream (assumed ready by 08:05):**
- Trusted analytical views (`trusted_views.a_*`)
- Refined analytical actuals (often from #67 spine and siblings)
- Trusted Odoo timestamped tables

**Downstream:**
- Sales / BI tools querying `refined_sales` only
- POS license activation report job (reads mart tables)
- Odoo active-asset / lifecycle exports that prefer the mart over full trusted

## Distinct from sibling patterns

| | 59 SFDC refined | 67 Refined SCD spine | 68 (this) |
|---|-----------------|----------------------|-----------|
| Role | Build CRM-shaped snapshots | Build analytical actuals + hist | Relocate objects into Sales mart |
| Transform | SQL aggregates / MD5 | Hash SCD2 + quarantine | None (identity copy) |
| Destination | `refined_salesforce` | `refined` | `refined_sales` |
| Operator mix | InsertJob (+ email) | Reserved InsertJob | InsertJob + BQ-to-BQ |
| Phase barrier | Staged fan-out | Sequential actuals | pause between view/table phases |
