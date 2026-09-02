# Business case: Invoice Radar

Subscription billing does not fail loudly. A suspended asset that
still invoices, a renewal whose LPV snap starts next month, or a
price book row that no longer covers `effective_pricing_date` all
show up as quiet euro gaps. Finance wanted a daily view of those
gaps before month-end, not a ticket after the close.

## Problem

LPV (lifecycle / planned value) and the invoice ledger live in
different BigQuery models. Analysts could join them ad hoc, but the
D-3 billing window, late-post rules, and "why is LPV zero?" taxonomy
were trapped in a notebook. Nobody re-ran it the same way twice, and
the Excel that leadership expected never landed in the same inbox.

## Approach

Promote the notebook into Composer with a deliberate split:

1. **Generate** — one Python task runs the BQ reconciliation, writes
   an optional `bi.all_invoices` snapshot (WRITE_TRUNCATE), builds a
   4-sheet workbook (missing invoice, missing LPV, under, over), and
   returns an email payload on XCom.
2. **Send** — a second task reads XCom and hands the payload to a
   shared SendGrid/SMTP helper.

Config is JSON in an Airflow Variable; SMTP/SendGrid secrets stay in
separate Variables. DEV forces a fixed recipient list so a bad
Variable cannot spam the business.

I kept the notebook's reason CASE rather than pushing it into dbt.
Finance iterates on wording weekly; shipping that through a dbt PR
was slower than editing the report module. The tradeoff is a large
SQL string in Python — acceptable for a control report, wrong for a
shared mart.

## Why two tasks?

Email providers time out. If generate and send were one callable,
every SendGrid blip would re-scan LPV. XCom keeps the Excel bytes
path and HTML on the worker disk; retries only re-send.

## Constraints

- Report date is "today in Europe/Amsterdam minus 3 days" so billing
  batches that post late still sit in the window.
- Full result set is held in pandas on the worker before Excel —
  fine for daily discrepancy volumes, not for raw ledger grains.
- `WRITE_TRUNCATE` on `bi.all_invoices` is a full refresh of the
  radar extract, not historical SCD.
- HTML template is string replace, not Jinja — keeps Composer
  dependencies thin.

## Out of scope

OCR / AlloyDB invoice-AI table extracts (`invoice_ai_data_import`)
are a separate ingest with different ownership. Partner invoice
event exports (pattern 08) are outbound product feeds, not this
control loop.
