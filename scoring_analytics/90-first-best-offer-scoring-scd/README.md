# Pattern 90: First Best Offer weekly scoring SCD

Weekly Composer DAG that boots a dedicated GCE scoring VM, runs two
First Best Offer predict jobs in parallel over SSH (wholesale account
grain + independent establishment grain), then historises each
population in BigQuery with keyhash/rowhash SCD Type 2.

Distinct from pattern 04 (monthly FBO/NBO partner Avro export),
pattern 47 (absolute activity scores), and pattern 67 (daily
refined-zone SCD spine). This folder is the *upstream weekly score
generation + historization* contract that those consumers read.

Source (read-only):
- `dags/horeca_digital/archived/etl_first_best_offer.py`

Related (not shipped here):
- `etl_first_best_offer_dev.py` — same graph against a `-dev` VM
- `etl_next_best_offer.py` — sibling NBO VM + SCD; natural next candidate

## Files

| File | Role |
|------|------|
| `dag_first_best_offer_scoring_scd.py` | VM lifecycle, SSH predicts, SCD task graph |
| `fbo_scd_queries.py` | Stage / expire / insert SQL builders + arm catalog |
| `BUSINESS_CASE.md` | Why VM predict + BQ SCD, and the all_done footguns |
| `ARCHITECTURE.md` | Components + Mermaid diagram |
| `DATA_FLOW.md` | Run order, hash contract, failure modes |

## Quick start

```bash
python -c "import ast; ast.parse(open('fbo_scd_queries.py').read())"
python -c "import ast; ast.parse(open('dag_first_best_offer_scoring_scd.py').read())"
python fbo_scd_queries.py
```

To run for real you need a GCE scoring VM with the predict package,
Composer Variables (`dwh_gcp_project`, `fbo_gce_*`, `composer_bucket`,
SSH key URI), refined score tables written by the model, and hist
tables with the SCD audit columns. This folder is a sanitized
reference, not a deploy package. The VM-side `predict_fbo.py` is not
included.

## Sanitization notes

- GCP project `hd-dwh-stream-1` → Variables `dwh_gcp_project` /
  `fbo_gce_project` (default `dwh_project`)
- Datasets `dwh_refined` / `dwh_trusted_staging` → `refined` /
  `trusted_staging`
- Tables `hd_fbo_metro_score*` / `hd_fbo_di_wo_metro_score*` →
  `fbo_wholesale_score*` / `fbo_independent_score*`
- Keys `metro_id` / `md_establishment_id` → `wholesale_account_id` /
  `establishment_id`
- Model flags `prod` / `di_wo_metro` → `wholesale` / `independent`
- VM / SSH key paths → Variables; private key material never committed
- Emails / authors → `dataops@example.com` / `data-platform`
- Inline SQL extracted into `fbo_scd_queries.py` with a shared
  `FboScoreArm` factory (production duplicated six operator blocks)
- Added `max_active_runs=1` and tags
- Kept production `trigger_rule=all_done` on historization and stop —
  document the footgun rather than silently "fix" behaviour

## Category

`scoring_analytics/90-first-best-offer-scoring-scd/`
