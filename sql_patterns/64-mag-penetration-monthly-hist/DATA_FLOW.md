# Data Flow: MAG penetration monthly historization

## Happy path

1. **Trigger** — cron `16 7 2 * *` (07:16 UTC on the 2nd).
2. **Country append** — SELECT prior-month country aggregates from
   `refined.vw_penetration_rates_reporting` →
   `WRITE_APPEND` into `refined.hist_penetration_rates_reporting`.
3. **Corp delta append** — SELECT establishment-level segment flags from
   `refined.platform_customer_base_establishment`, subtract same-month
   hist sums, append one `country = 'corp'` row to the same hist table.
4. **Downstream** — pattern 24 (and other MAG readers) consume the hist
   table on their own schedules.

## Paths

| Path | What moves | Write mode |
|------|------------|------------|
| A Country actuals | live view → hist | WRITE_APPEND |
| B Corp delta | customer base − hist sums → hist | WRITE_APPEND |

## Reporting month

```
reporting_month = DATE_SUB(DATE_TRUNC(CURRENT_DATE, MONTH), INTERVAL 1 MONTH)
```

Both queries pin every output row to that date. There is no Airflow
execution-date templating — the SQL uses `CURRENT_DATE` at query time.
A manual re-run mid-month still targets the *prior* calendar month.

## Failure modes

| Failure | Effect | Recovery |
|---------|--------|----------|
| Country append fails | Corp still scheduled (`ALL_DONE`); may write a bad corp row | Delete bad month rows; fix view; re-run both tasks |
| Corp append fails | Country rows present; corp missing for the month | Re-run corp task only after confirming country rows |
| Clear + full re-run same month | Duplicate country + corp rows | Delete `WHERE date = reporting_month` then re-run |
| Upstream view stale on 2nd | Hist locks wrong actuals | Wait for view refresh; delete month; re-run |
| Prior run still active | `max_active_runs=1` blocks | Wait / mark failed after investigation |

## Upstream / downstream

**Upstream (must be fresh before the 2nd):**
- Daily refined zone / penetration view builders
- `platform_customer_base_establishment` product-flag land
- Preferably the 1st-of-month MAG sales/acquisitions hist sibling

**Downstream:**
- Pattern 24 partner penetration export
- VCD / group MAG reporting that reads
  `hist_penetration_rates_reporting`

## Distinct from related patterns

| | 24 MAG export | 46 / 48 / 62 zones | 64 (this) |
|---|---------------|--------------------|-----------|
| Cadence | monthly export | daily / bi-monthly rebuild | monthly append (2nd) |
| Write | Avro → partner API | CREATE OR REPLACE / fan-out | WRITE_APPEND hist |
| Question | ship rates outbound | rebuild analytics zones | freeze month-end rates |
| Corp logic | none (reads hist) | n/a | delta vs prior hist |

## Sibling left for later

Archived `etl_refined_zone_monthly` (1st of month): sales +
acquisitions hist append and SFDC Metro ID clean. Related calendar,
different tables — ship separately if depth warrants.
