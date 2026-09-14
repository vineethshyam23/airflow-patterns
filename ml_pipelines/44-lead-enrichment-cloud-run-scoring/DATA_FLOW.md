# Data flow: Lead enrichment + Cloud Run scoring

## Daily path (happy case)

1. **05:30 UTC** — schedule fires; one run (`max_active_runs=1`).
2. **check_matching_engine_results** — `COUNT(*)` on
   `matching_engine.sam_leads` (or Variable override) where
   `DATE(_create_ts) = CURRENT_DATE()`.
3. **dbt_lead_enrichment_run** — dbt Cloud job builds
   `crm_spot.leads_enrichment` from matching-engine output.
4. **score_leads** — `CloudRunExecuteJobOperator` starts
   `lead-scoring-score` in the scoring project with args:
   `product`, `market`, `env`, `{{ data_interval_end | ds }}`,
   `mode`, `dry_run`. Task waits up to 1 hour for job exit.
5. **dbt_lead_scoring_run** — dbt Cloud job builds
   `crm_spot.leads_enrichment_final` after scores land.
6. **send_leads_enrichment_odoo** — pattern 02 lead engine reads today's
   `load_date` rows and creates/updates Odoo `crm.lead`.
7. **slack_notification_complete** — short success message to CRM ops.
8. **end**

## Soft-skip path

If the gate finds zero rows, or the BQ check raises, the branch goes to
`skip_enrichment` → `end`. No dbt, no Cloud Run, no Odoo, no Slack
success alert. That is intentional: empty matching-engine days are
normal; treating them as failures trained people to ignore alerts.

## Failure modes

| Failure | Behaviour | Ops action |
|---------|-----------|------------|
| Matching table missing / IAM | Gate catches → skip | Fix table Variable / BQ IAM; re-run when ready |
| dbt enrichment fail / timeout | Chain stops before scoring | Fix dbt job; clear from enrichment |
| Cloud Run job non-zero exit | `score_leads` fails | Check scoring logs / image args / IAM |
| Cloud Run provider missing | EmptyOperator stub (reference) | Install provider; set job Variables |
| dbt post-score fail | Odoo not called | Fix scored model; clear from post-score |
| Odoo RPC / mapping error | Push task fails | Creds Variable / pattern 02 mapper |
| Overlapping run | Blocked by `max_active_runs=1` | Wait or fail prior run intentionally |

## Idempotency

Re-running after a mid-chain clear re-executes from the cleared task.
dbt jobs are full model rebuilds for the enrichment/scoring packs.
Odoo push filters `load_date = CURRENT_DATE()` — a second successful
push the same day depends on the lead engine's create-vs-update
behaviour in pattern 02 (dedupe on business keys), not on this DAG.

Cloud Run date arg uses `data_interval_end | ds` so a manual clear
still passes a stable logical date into the scoring CLI.

## What this flow does not do

- Does not train or deploy the scoring model — only executes the
  deployed Cloud Run Job.
- Does not pull field-sales activities (pattern 42).
- Does not classify POS product text (pattern 02 ML pipeline).
- Does not implement Odoo field mapping — stub + pattern 02.
