# Business Case: MAG sales / acquisitions monthly historization

Group MAG reporting needs a frozen prior-month picture of product-bundle
sales and acquisitions — counts that do not drift when CRM opportunity
rows get late edits. Matching engine teams in FR/RO also need a cleaned
wholesale partner ID derived from free-text CRM fields that sales reps
typed inconsistently for years.

I kept these as one Composer DAG on the **1st of the month** rather than
folding them into the daily refined spine. Historization is a calendar
contract: close the prior month once, append, move on. Partner-ID
cleaning is a full rebuild (`WRITE_TRUNCATE`) and can safely re-run;
sales/acquisitions appends cannot — that asymmetry is why the Metro /
partner clean sits on the same schedule but **without** a dependency
edge onto the MAG chain.

The acquisitions query is the interesting piece. It does not just count
new sales — it carries `sales_all_time` forward from the previous hist
month via a full-outer join, so a country×bundle with zero new sales
still advances the cumulative. That is the contract VCD and MAG export
consumers historically expected. Penetration hist (pattern 64) runs a
day later on a different table with a different corp-delta rule; do not
collapse them.

## What this unlocks

- Append-only month grain on `refined.hist_sales_reporting` and
  `refined.hist_acquisitions_reporting`.
- Cumulative `sales_all_time` continuity across months without a
  separate SCD layer.
- A rebuilt `refined.crm_establishment_clean_partner_id` table that
  matching engine jobs can join without re-implementing ~10 regex
  patterns per country.

## Tradeoffs I accepted

- `WRITE_APPEND` with no delete-for-month guard. Clear+rerun duplicates
  the reporting month. Cheap ops, expensive incident review — same
  class of risk as pattern 64.
- `TriggerRule.ALL_DONE` on every task (production). Acquisitions can
  still append after a failed sales task. Left visible, not "fixed"
  for the portfolio.
- `retries: 0` in production. Monthly jobs that append should not
  auto-retry into a double-write. I kept that.
- ~340 lines of partner-ID SQL still live in the DAG path (extracted
  here into a builder). Moving it to dbt would not remove the Airflow
  schedule or the truncate contract.
- Archived source had `schedule_interval=None` after CRM raw land froze.
  Portfolio restores `15 7 1 * *` so the intended calendar stays
  readable.

## Not this pattern

- Pattern 24: outbound Avro export of MAG hist tables.
- Pattern 64: 2nd-of-month penetration hist with corp delta — sibling
  calendar, different tables and math.
- Pattern 01 / 10: matching-engine SCD and partner export — those
  *consume* the cleaned partner IDs this pattern produces.
- Pattern 59: SFDC refined daily aggregates — different CRM fan-out.
