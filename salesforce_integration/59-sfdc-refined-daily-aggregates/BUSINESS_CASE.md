# Business case: Salesforce refined daily aggregates

## Problem

Salesforce and internal reporting need establishment-level facts that
do not live in CRM: food-order revenue, reservation success rates,
subscription billing starts, voucher catalogs, Odoo wholesale invoice
lines mapped to SFDC assets, app engagement, and multi-country POS
throughput. Those facts sit across refined views and trusted country
tables. Asking CRM (or a BI tool) to join them live is slow, brittle,
and burns BigQuery slots on every dashboard refresh.

I needed one Composer DAG that lands a small, CRM-shaped dataset each
morning so Salesforce integrations and reporting can treat warehouse
tables as the contract — not reinvent the joins.

## Approach that stuck

Eight `BigQueryInsertJobOperator` tasks, each `WRITE_TRUNCATE` into
`refined_salesforce.*`. Fan-out in two stages (orders + reservations
first; billing / vouchers / Odoo / app login next; POS last because the
four-country union is the expensive one), then an email when the DAG
finishes.

The Odoo→SFDC revenue query is the interesting bit: join SFDC assets
to Odoo WSL invoice lines on order UID + product base code, `QUALIFY`
to one row per Odoo id, and emit an MD5 `_rowhash` so a later upsert
into Salesforce establishment-revenue objects can skip unchanged rows.

## Tradeoffs I accepted

- **Full daily truncate** instead of incremental MERGE. Downstream
  consumers treat these as morning snapshots. Truncate-reload is easier
  to reason about when an upstream join changes shape than debugging
  a half-merged grain.
- **`TriggerRule.ALL_DONE` on most tasks.** Production kept the DAG
  moving when a sibling failed so reporting still got a completion
  email. That is an operability smell — partial refresh can look
  "green". Left visible in the sample; flip to `ALL_SUCCESS` if you
  want fail-closed.
- **Inline SQL lived in the DAG for years.** I extracted builders so
  the pattern is reviewable; production still ships as one file.
- **POS as a four-way `UNION DISTINCT`.** Country tables stayed
  separate in trusted. A single partitioned POS fact would be cleaner;
  this DAG reflects the warehouse we had.

## What this is not

Not the asset-history delta export (pattern 05) — that pushes hash
deltas to an external event bus. This DAG only materializes warehouse
tables for CRM/reporting consumers. It does not call Salesforce APIs.
