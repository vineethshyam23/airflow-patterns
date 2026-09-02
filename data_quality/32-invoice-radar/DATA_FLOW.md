# Data flow: Invoice Radar

## Schedule

- DEV: manual (`schedule=None`)
- PROD: `0 9 * * *` Europe/Amsterdam

## Report window

Computed in `Europe/Amsterdam`, not worker UTC:

| Label | Value |
|-------|-------|
| report_date | today − 3 days |
| next_day | today − 2 days |
| next_next_day | today − 1 day |
| next_month_report | report_date + 1 calendar month (clamped) |
| cal_month filter | month(s) covering report_date … next_next_day |

Rows enter the extract when `next_invoice_date` or `last_invoice_date`
hits that window (including late posts where last invoice is D-2/D-1
but next invoice aligns to report_date + 1 month).

## Buckets

| Bucket | Rule (simplified) |
|--------|-------------------|
| missing_inv | next invoice = report_date, LPV > 0, invoice = 0 |
| missing_lpv | invoiced in window, LPV = 0, invoice > 0 |
| under | invoiced, both > 0, invoice € < LPV € |
| over | invoiced, both > 0, invoice € > LPV € |

Excel sheets follow the same four buckets plus a Summary sheet
rolled up by country × product × one-time/recurring.

## Steps

1. **load_invoice_radar_environment** — Variables → env (inside
   generate and again inside send).
2. **BigQuery extract** — LPV left join latest asset activator +
   pricing windows; CASE taxonomy for zero-LPV explanations.
3. **Optional load** — `WRITE_TRUNCATE` into `bi.all_invoices` when
   `WRITE_ALL_INVOICES` is true.
4. **Bucket + Excel** — openpyxl workbook to staging
   `{staging}/{run_id}/invoice_radar_{date}.xlsx`.
5. **HTML** — fill `invoice_radar_alert.html` counters.
6. **XCom** — list with one payload (recipients, subject, html,
   attachment path, plain summary).
7. **Send** — EmailDelivery via SendGrid or SMTP; any send failure
   increments a counter and raises after the loop.

## Failure modes

| Case | Behaviour |
|------|-----------|
| Missing Variable / secret | fail at env load |
| BQ query error | fail generate; no email |
| Empty extract | empty sheets still emailed; BQ write skipped |
| SendGrid/SMTP error | log per payload; raise if any failed |
| DEV misconfig | recipients forced to `DEV_TEST_RECIPIENTS` |

## Re-run

Safe to clear generate + send and re-run. Truncate load replaces
`bi.all_invoices` with the new extract. Email will go out again —
coordinate with finance on PROD re-runs.
