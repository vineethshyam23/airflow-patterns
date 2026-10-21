# Business case: LPV Adjustment monthly report

Finance needs last month's LPV adjustment grain as country-split
CSVs on a fixed calendar — not a reconciliation against invoices,
and not an ad-hoc notebook export that only one analyst can re-run.

## Problem

`discovery.lpv_adjusted` already holds the adjusted lifecycle value
rows. What was missing was a reliable monthly delivery: previous
calendar month, Recurring vs Onetime from `product_is_setup`, one
CSV per `country_code`, zipped, emailed by the 5th. The notebook
worked once; it did not survive Composer secrets, SendGrid size
limits, or recipient-list drift.

## Approach

Promote the notebook into Composer with the same generate→XCom→send
split used by Invoice Radar (pattern 32), but a different contract:

1. **Generate** — query previous calendar month, classify revenue
   model in SQL, build a DEFLATE zip of per-country CSVs, stage
   under `{staging}/{run_id}/`, return one email payload on XCom.
2. **Send** — pull XCom, optionally offload attachments larger than
   20 MB to the Composer GCS bucket (SendGrid hard limit), then
   deliver via SendGrid or SMTP.

Config JSON stays free of passwords. Recipient list is pinned in
code so a leftover Variable cannot silently drop stakeholders.

## Why not reuse Invoice Radar?

Invoice Radar answers "what is wrong today?" with a D-3 billing
window and four discrepancy buckets. This DAG answers "what did
last month look like by country?" for planning and audit. Different
SLA (monthly 5th), different attachment shape (zip of CSVs), and
an empty month is a success with no email — not an empty workbook.

## Constraints

- Window is `DATE_TRUNC(CURRENT_DATE(), MONTH) - 1 month` in BQ,
  not a rolling D-n window.
- Full month sits in pandas on the worker before zip — acceptable
  for adjustment volumes, wrong for raw ledger grains.
- Recipients are forced; transport settings come from Variables.
- Oversized zips become GCS links in the HTML body rather than
  failing the run.

## Out of scope

Daily LPV vs invoice reconciliation (pattern 32). Partner invoice
event exports (pattern 08). OCR / AlloyDB invoice-AI ingest.
