# Business case: Mach2 Odoo sales email report

POS and add-on lifecycle changes do not wait for a weekly BI pack.
Partner ops and sales channel owners need yesterday's activations,
cancellations, and channel attach events as Excel they can act on
before the morning standup — with fiscal IDs and CRM matching keys
already joined.

## Problem

The warehouse already holds refined Odoo partners, orders, lines, and
product templates. What was missing was a stable, scheduled cut of
that data into audience-specific workbooks. Notebooks drifted: one
person filtered Mach2 basic licenses, another forgot the partner
matching join, and fiscal UUID defaults lived in someone's head.

Invoice Radar (pattern 32) solves a different question — expected
subscription revenue vs posted invoices. Mixing those audiences into
one DAG would either spam finance with POS SKU noise or bury revenue
gaps under activation lists.

## Approach

Promote the notebook into Composer with the same generate→send split
I used on Invoice Radar:

1. **Generate** — one task runs five BigQuery report specs, writes
   timestamped Excel under a run-scoped staging dir, and returns email
   payloads on XCom (recipients, HTML, attachment path).
2. **Send** — a second task walks XCom and calls the shared
   SendGrid/SMTP helper. A provider timeout retries send only.

Config is JSON in an Airflow Variable (table names, audiences,
provider). Secrets stay in separate Variables. DEV forces a fixed
recipient list so a bad Variable cannot email the business.

I kept product-code CASE maps and fiscal UUID defaults in the report
module rather than dbt. Ops tweaks SKU aliases weekly; a dbt PR for
every rename was slower than editing the report. The tradeoff is a
large SQL string in Python — fine for an operational pack, wrong for
a shared mart.

## Why five reports, three audiences?

Sales channels, add-on activations, and add-on cancellations are
different operational queues. POS activation / cancellation carry
fiscal hardware fields the channel reports do not need. Splitting
workbooks keeps each inbox useful; sharing one generate task keeps
the warehouse query session and staging layout in one place.

## Constraints

- Window is "created / closed yesterday" in Europe/Amsterdam terms —
  late evening Odoo writes still land in the morning pack.
- Full result sets are held in pandas on the worker before Excel —
  acceptable for daily POS deltas, not for raw order-line history.
- Empty result sets still send HTML (no attachment) so silence is
  distinguishable from a failed DAG.
- Partner matching is a left join from refined CRM keys; missing
  matches stay in the sheet rather than dropping the row.

## Out of scope

Invoice Radar LPV reconciliation, DishPay KYC/transaction pulls, and
raw Odoo Postgres CDC are separate patterns. This DAG reads refined
BigQuery only.
