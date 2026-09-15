# Business case: Food Graph Vertex PipelineJob

## Problem

The Food Graph training / processing graph lives as a compiled Kubeflow
pipeline on GCS. Data scientists iterate on step parameters (which
stages run for a market / environment) faster than we can ship DAG
code. Ops still needs a reliable daily trigger, a known service
account, and an auditable set of flags for acceptance vs production.

If every flag change required a DAG PR, we either froze the pipeline
or bypassed Composer and ran notebooks against Vertex directly —
neither scales past one market.

## Decision

One Composer DAG owns the *submission contract*:

1. **Template URI from Variable** — compiled pipeline JSON stays in
   GCS; Composer downloads a fresh copy each run so template bumps do
   not need a DAG redeploy.
2. **Per-step enable flags as Variables** — gold ingredient processor,
   gap analysis, recipe extraction, purchase frequency, etc. flip
   without editing Python. Defaults match the acceptance schedule
   (gold on; heavier stages off).
3. **`PipelineJob.submit` fire-and-forget** — Vertex owns runtime and
   retries inside the pipeline. Composer records "submit succeeded"
   and moves on. That is the opposite of pattern 44 (Cloud Run wait).
4. **GCP connection extras for GCS auth** — same Airflow connection
   shape the rest of the DWH uses; no second secret store for the
   template download.

Pattern 40 still owns landing Vertex *outputs* into DWH / recommender
tables. This DAG does not copy BigQuery results; it starts the job
that produces them.

## Constraints I cared about

- Template download and submit must share the same project / SA story.
  Splitting "download as DWH SA, submit as Vertex SA" created silent
  IAM failures that looked like GCS 403s.
- Logging connection key material is unacceptable. Production once
  printed `keyfile_dict` into task logs; this reference never does.
- `max_active_runs=1` — overlapping submits with different flag
  matrices made Vertex job history hard to reason about.
- String `"True"` / `"False"` for step parameters match the compiled
  KFP contract. Coercing to Python bool broke parameter validation on
  older template revisions.
- Caching off by default for acceptance — stale cached steps hid data
  bugs when upstream gold tables changed.

## Outcome

Acceptance (and later prod) can change which Food Graph stages run by
editing Variables, not by redeploying Composer. Vertex job history
stays attributable to a single DAG id. Pattern 40 continues to land
outputs on its own schedule once the pipeline writes them.
