# Business case: sale_order_line snapshot soft-close

Daily Odoo lands rewrite current rows. They do not notice deletes.
Downstream partners (and our own refined marts) treat
`dbt_valid_to IS NULL` as "still live in ERP." When ops hard-deletes a
line in Odoo, that open snapshot row becomes a silent lie until
someone notices a weekly asset export joining ghosts.

I wanted delete detection without hard-DELETEing warehouse history and
without stuffing a multi-million ID list through XCom. Soft-close via
`dbt_valid_to = CURRENT_TIMESTAMP()` keeps the dbt snapshot contract.
Landing IDs through GCS → BigQuery temp table is the boring scalable
path: the extract is `SELECT id` only, the join stays in BQ, and
Composer metadata stores counts + a sample — not the full set.

Why weekly, not daily? Delete lag of a few days was acceptable for the
consumers that care (partner active-ID filters, refined sales joins).
Daily full ID pulls would burn Postgres and Composer for little SLA
gain. Sundays at 02:00 UTC put the job ahead of Monday asset exports
that already assume a clean open set.

Tradeoffs I accepted:

- No dry-run flag — identify logs samples, then UPDATE commits. Wrong
  closes need a manual `dbt_valid_to = NULL` fix.
- Identify and update are two BQ passes over the same LEFT JOIN. Clear
  audit trail beat a single MERGE for the first production cut.
- Sync bands (<100 / 100–1000 / >1000) are log guidance, not hard
  fails. Timing skew around the daily land can look like a minor gap.

Distinct from pattern 13 (push active IDs outbound) and 60 (write
stock.picking into Odoo): this job only repairs warehouse snapshot
validity against live ERP presence.
