# Pattern 44: Lead enrichment + Cloud Run scoring

Composer DAG that gates on same-day Vertex matching-engine output,
runs dbt enrichment, executes a Cloud Run scoring job (wait for exit),
rebuilds the scored leads model, and pushes to Odoo CRM.

Distinct from pattern 02 (POS text classification / Odoo lead class)
and pattern 42 (field-sales API → CRM). This pattern owns the
enrichment + batch-scoring handoff between matching engine, Cloud Run,
and CRM.

Source (read-only):
- `dags/etl_leads_enrichment.py`
- uses `horeca_digital/lead_engine_odoo.py` (covered by pattern 02)

## Files

| File | Role |
|------|------|
| `matching_engine_gate.py` | Same-day BQ count → proceed or soft-skip |
| `dag_lead_enrichment_scoring.py` | Branch, dbt, Cloud Run, Odoo, Slack |
| `odoo_enrichment_push.py` | Thin adapter; wire pattern 02 in prod |
| `BUSINESS_CASE.md` | Why one DAG owns enrich → score → CRM |
| `ARCHITECTURE.md` | Components + Mermaid diagram |
| `DATA_FLOW.md` | Happy path, soft-skip, failure modes |

## Quick start

```bash
python -c "import ast; ast.parse(open('matching_engine_gate.py').read())"
python -c "import ast; ast.parse(open('odoo_enrichment_push.py').read())"
python -c "import ast; ast.parse(open('dag_lead_enrichment_scoring.py').read())"
```

To run for real you need Airflow Variables for DWH / scoring projects,
matching-engine table FQN, dbt job ids, Cloud Run job name/region,
Odoo JSON creds, Slack webhook, and the pattern 02 lead engine wired
into `odoo_enrichment_push.py`. This folder is a sanitized reference,
not a deploy package.

## Sanitization notes

- GCP projects `hd-dwh-stream-*` / `hd-dwh-vertex-prod-*` →
  `dwh_project` / `ml_scoring_project` (Variables)
- Dataset `dish_sms_spot` → `crm_spot`
- Table `matching_engine_prod.matching_engine_sam_leads` →
  `matching_engine.sam_leads` (Variable override)
- Hardcoded dbt job ids → `lead_enrichment_dbt_job_id` /
  `lead_scoring_dbt_job_id`
- Cloud Run job `lead-scoring-score-dev` → Variable
  `lead_scoring_job_name` (default `lead-scoring-score`)
- Emails / Slack channels / owners → `dataops@example.com` /
  `#crm-data-ops` / `data-platform`
- Removed Slack emoji from success message
- Package import `horeca_digital.lead_engine_odoo` → thin local stub
- `DummyOperator` → `EmptyOperator`
- dbt / Cloud Run providers optional with EmptyOperator stubs
- Kept wait-for-exit Cloud Run behaviour and soft-skip gate
- `max_active_runs=1`

## Category

`ml_pipelines/44-lead-enrichment-cloud-run-scoring/`
