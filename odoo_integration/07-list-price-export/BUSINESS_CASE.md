# Business case: Odoo list-price / commission monthly delta export

Finance and partner analytics need Odoo invoice-line economics — oneshot vs
recurring, list price vs invoiced, partner commission splits, promotions —
as events on a monthly cadence. They do not need a nightly dump of every
open bill.

This pattern is the bridge: materialize a month-start snapshot of WSL
invoice lines joined to the internal list-price book, detect what changed
since the last successful run with keyhash/rowhash, and push only the delta
as Avro events. An SCD-style history table keeps the audit trail for
"what commission left the platform on bill X."

## What this unlocked

- Monthly Odoo billing → event bus without replaying the full invoice
  universe every night
- Hash-based change detection so quiet months (few credit notes / price
  book edits) cost almost nothing on the API
- A history table that keeps superseded commission versions for dispute
  and rebate questions without rebuilding from Odoo Postgres

I cared about the schedule as much as the SQL. Commission consumers settle
monthly; daily hashing of mostly-static bills burns quota and creates
noise in finance ops Slack. Monthly on the 1st after month-end close is the
honest cadence.

## Constraints

- Schedule is monthly (`55 2 1 * *`), after refined WSL invoice lines land.
  Mid-month credit notes wait for the next cycle unless you force a manual
  run with the same DAG (catchup stays off on purpose).
- Pilot started on one market (FR). The country list and ISO map are left as
  structures so a second market can be added without reshaping the DAG.
- Avro bulk posts in chunks of 500 to match the shared event-API contract.
- Key identity is parent_bill + salesforce_establishment_id. Amounts and
  product codes live in the rowhash — a credit note on the same bill
  produces a new event without inventing CDC on Odoo.

## What this is not

Not Odoo → warehouse extraction (that lives upstream of the refined WSL
table). Not invoice write-back into Odoo. It is warehouse → event bus for
list-price / commission measures, with an SCD-lite side effect so we can
prove what changed each month.
