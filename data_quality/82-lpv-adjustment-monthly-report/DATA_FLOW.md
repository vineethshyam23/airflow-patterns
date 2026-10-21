# Data flow: LPV Adjustment monthly report

## Schedule

- DEV: manual (`schedule=None`)
- PROD: `0 9 5 * *` Europe/Amsterdam (5th of month, 09:00)

## Report window

Computed in BigQuery against `CURRENT_DATE()`:

| Label | Value |
|-------|-------|
| cal_month filter | `DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 MONTH)` |
| period label | month name + year from first `cal_month`, else prior month in Europe/Amsterdam |

No D-n billing window. Entire previous calendar month, all countries
in one query.

## Extract shape

| Field / derived | Role |
|-----------------|------|
| `cal_month` | month grain |
| `country_code` | zip member key |
| `revenue_model` | `Recurring` when `product_is_setup` is false; `Onetime` when true |
| remaining columns | passthrough via `* EXCEPT (...)` |

## Steps

1. **load_lpv_environment** — Variables → env (inside generate and
   again inside send); pin recipients.
2. **BigQuery extract** — previous month from `discovery.lpv_adjusted`.
3. **Empty gate** — zero rows → return `[]`; DAG succeeds, no email.
4. **Country zip** — one UTF-8 CSV per `country_code`, DEFLATE level 9,
   staged as `LPV_Adjustment_{Month_YYYY}.zip`.
5. **HTML** — inline summary with row count, country count, period.
6. **XCom** — list with one payload (or empty).
7. **Send** — if zip > 20 MB, upload to `gs://{composer_bucket}/lpv_report/`
   and rewrite HTML with the URI; then EmailDelivery.

## Failure modes

| Case | Behaviour |
|------|-----------|
| Missing Variable / secret | fail at env load |
| BQ query error | fail generate; no email |
| Empty previous month | success; no email |
| Zip > 20 MB | GCS offload; email without attachment |
| SendGrid/SMTP error | log per payload; raise if any failed |
| Recipient drift in JSON | ignored; `REPORT_RECIPIENTS` wins |

## Re-run

Safe to clear generate + send and re-run. Email will go out again
with a fresh zip — coordinate with finance on PROD re-runs after
month close.
