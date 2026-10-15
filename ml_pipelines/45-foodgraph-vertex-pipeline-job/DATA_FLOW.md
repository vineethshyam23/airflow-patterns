# Data flow: Food Graph Vertex PipelineJob

## Daily path (happy case)

1. **@daily** — schedule fires; one run (`max_active_runs=1`).
2. **Resolve Variables** — project, template URI, pipeline root,
   service account, ISO code, env, and per-step `"True"`/`"False"`
   flags.
3. **Load GCP connection extra** — `BaseHook.get_connection` for GCS
   download credentials (or ADC in reference environments).
4. **Download template** — `gs://…/foodgraph_training_pipeline.json`
   → local `/tmp/foodgraph_vertex_pipeline/…`.
5. **Submit PipelineJob** — `aiplatform.PipelineJob(...).submit(sa)`.
   Task succeeds when Vertex accepts the job, not when the pipeline
   finishes.
6. **Vertex runtime** — enabled steps run under the pipeline root;
   artifacts land in GCS; BQ outputs appear when those steps write
   them.
7. **Pattern 40 (separate schedule)** — later propagates gold /
   preprocessed tables into DWH / recommender projects.

## Step flag matrix (defaults)

| Flag Variable suffix | Vertex parameter | Default |
|----------------------|------------------|---------|
| `gold_ingredient_processor_flag` | `Step_1_…` | True |
| `data_preprocessor_flag` | `Step_2_…` | False |
| `ingredient_normaliser_flag` | `Step_3_…` | False |
| `articles_to_ing_flag` | `Step_4_…` | False |
| `recipes_to_ing_flag` | `Step_5_…` | False |
| `menu_items_to_recipe_flag` | `Step_6_…` | False |
| `purchase_frequency_flag` | `Step_7_…` | False |
| `gap_analysis_flag` | `Step_8_…` | False |

Acceptance typically runs Step_1 only. Heavier stages are flipped on
for controlled market refreshes without a DAG code change.

## Failure modes

| Failure | Behaviour | Ops action |
|---------|-----------|------------|
| GCS template 404 / IAM | Download raises → task fail | Fix template Variable / bucket IAM |
| Connection missing key material | Falls back to ADC; may 401 | Set GCP connection extras |
| Vertex submit IAM | `submit()` raises | Grant pipeline SA `aiplatform.user` + SA user |
| Bad flag type (bool vs string) | Parameter validation error | Keep `"True"`/`"False"` strings |
| Pipeline fails after submit | Composer already green | Monitor Vertex console / alerting there |
| Overlapping run | Blocked by `max_active_runs=1` | Wait or clear prior run |

## Idempotency

Each submit uses a timestamped `job_id`
(`foodgraph-pipeline-YYYY-MM-DD-HH-MM-SS`). Re-running creates a new
Vertex job; it does not cancel the previous one. Clear carefully if a
partial flag matrix was wrong — cancel in Vertex, then re-submit.

## What this flow does not do

- Does not wait for pipeline completion (unlike pattern 44 Cloud Run).
- Does not copy BQ outputs into DWH (pattern 40).
- Does not compile the KFP graph — assumes a published template URI.
- Does not score CRM leads (pattern 44) or classify POS text (pattern 02).
