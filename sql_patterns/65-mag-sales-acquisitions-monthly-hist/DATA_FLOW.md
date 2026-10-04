# Data Flow: MAG sales / acquisitions monthly historization

## Happy path

1. **Trigger** — cron `15 7 1 * *` (07:15 UTC on the 1st).
2. **Sales append** — SELECT prior-month bundle counts from
   `refined.vw_acquisitions_base` → `WRITE_APPEND`
   `refined.hist_sales_reporting`.
3. **Acquisitions append** — SELECT prior-month acquisitions, join to
   previous hist month's `sales_all_time`, append to
   `refined.hist_acquisitions_reporting`.
4. **Partner-ID clean (parallel)** — regex-match CRM `Partner_Id__c`
   for FR/RO, validate against wholesale customers →
   `WRITE_TRUNCATE` `refined.crm_establishment_clean_partner_id`.
5. **Downstream** — VCD / MAG export / matching engine / pattern 64
   (next day) on their own schedules.

## Paths

| Path | What moves | Write mode |
|------|------------|------------|
| A Sales hist | base view → hist_sales | WRITE_APPEND |
| B Acquisitions hist | reporting view + prior hist → hist_acq | WRITE_APPEND |
| C Partner-ID clean | CRM × wholesale → clean table | WRITE_TRUNCATE |

## Reporting month

```
reporting_month = DATE_SUB(DATE_TRUNC(CURRENT_DATE, MONTH), INTERVAL 1 MONTH)
```

SQL uses `CURRENT_DATE` at query time — no Airflow execution-date
templating. A mid-month manual re-run still targets the prior calendar
month.

## Failure modes

| Failure | Effect | Recovery |
|---------|--------|----------|
| Sales append fails | Acquisitions still scheduled (`ALL_DONE`); may write without sales peer | Fix view; delete bad month from both hist tables; re-run chain |
| Acquisitions append fails | Sales rows present; cumulative missing | Re-run acquisitions after confirming sales rows |
| Clear + full re-run same month | Duplicate hist rows | `DELETE WHERE date = reporting_month` then re-run |
| Partner-ID clean fails | MAG hist unaffected (no edge) | Re-run truncate task alone — safe |
| Upstream CRM / wholesale stale | Clean table under-matches | Wait for land; re-run truncate |
| Overlapping monthly run | Blocked by `max_active_runs=1` | Wait / mark failed after investigation |

## Upstream / downstream

**Upstream (must be fresh before the 1st):**
- CRM opportunity / acquisition views feeding the MAG base views
- Trusted wholesale customer tables for FR/RO
- CRM establishment land (`Partner_Id__c`, `Store__c`)

**Downstream:**
- Pattern 24 MAG acquisition + penetration export
- VCD jobs that copy `hist_sales_reporting`
- Matching engine production (reads cleaned partner IDs)
- Pattern 64 penetration hist (2nd of month)

## Distinct from related patterns

| | 24 MAG export | 64 penetration hist | 65 (this) |
|---|---------------|---------------------|-----------|
| Cadence | monthly export | monthly append (2nd) | monthly append (1st) |
| Write | Avro → partner API | WRITE_APPEND penetration | WRITE_APPEND sales/acq + TRUNCATE IDs |
| Question | ship hist outbound | freeze penetration + corp delta | freeze sales/acq + clean partner IDs |
| Cumulative | none (reads hist) | corp delta vs prior hist | sales_all_time carry-forward |
