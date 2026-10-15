# Business case: Lead enrichment + Cloud Run scoring

## Problem

Vertex matching-engine output and a dbt SAM leads job land new
candidates every morning. Sales needs those leads enriched, scored for
a product/market, and written into Odoo the same day — not sitting in
warehouse tables until someone notices.

Running enrichment SQL, calling a scoring service, rebuilding the
scored model, and pushing CRM records as three disconnected jobs
created race conditions: Cloud Run fired before enrichment finished,
or Odoo read yesterday's scored table after a partial dbt failure.

## Decision

One Composer DAG owns the contract:

1. **Gate on matching-engine rows for today** — BranchPythonOperator
   soft-skips the whole enrichment path when Vertex produced nothing.
   Quiet days stay quiet; they do not page as pipeline failures.
2. **dbt enrichment** — builds the leads enrichment model from matching
   engine output (job id via Variable).
3. **Cloud Run Execute Job** — triggers the lead-scoring container and
   *waits* for the job exit code. Success/failure of the task is the
   scoring job's exit status, not a fire-and-forget HTTP 200.
4. **dbt post-score** — rebuilds the final scored leads table only after
   scores land.
5. **Odoo push** — same lead-engine class as pattern 02, pointed at the
   enrichment-final table for today's `load_date`.
6. **Slack on success** — short confirmation for CRM ops.

This is not pattern 02 (POS sklearn text classification / Odoo class
itself) and not pattern 42 (field-sales API ingest into CRM). Pattern
42 feeds leads from an activities API; this DAG scores matching-engine
leads and then writes CRM.

## Constraints I cared about

- Scoring project is often a *different* GCP project from the DWH
  billing project — Variables separate `lead_scoring_gcp_project` from
  `dwh_project`.
- Waiting on Cloud Run matters. An earlier version fired the job and
  continued; IAM `run.developer` plus status check made wait-for-exit
  the correct default.
- Soft-skip on gate query errors: a BQ permission blip should not
  cascade into a fake "score then push empty" path. Skip is safer than
  proceeding on uncertainty.
- `max_active_runs=1` — enrichment + hour-long scoring must not overlap.
- Job name / image env (`prod` vs `dev`) pinned via Variables so the DAG
  can stay on a verified scoring revision until prod IAM is ready.
- Odoo mapping stays in pattern 02; do not fork a second lead mapper
  inside this folder.

## Outcome

Matching-engine days produce enriched, scored leads in Odoo on a
predictable morning SLA. Empty days exit cleanly. Ops has one graph to
clear when scoring IAM or dbt drifts, instead of four scripts and a
Slack argument about who ran first.
