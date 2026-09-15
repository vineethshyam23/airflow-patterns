# Pattern 45: Food Graph Vertex PipelineJob

Composer DAG that downloads a compiled Kubeflow pipeline JSON from
GCS and submits a Vertex AI `PipelineJob` with per-step enable flags
(gold ingredients, gap analysis, recipe extraction, …).

Distinct from pattern 40 (BQ gold + cross-project copy of ML *outputs*)
and pattern 44 (Cloud Run Execute Job wait-for-exit scoring). This
pattern owns *pipeline submission* — schedule, template URI, and the
step-flag contract.

Source (read-only):
- `dags/horeca_digital/archived/etl_food_graph_vertex.py`
- `dags/horeca_digital/food_graph_vertex.py`
- `dags/horeca_digital/food_graph_vertex_utils.py`

## Files

| File | Role |
|------|------|
| `dag_vertex_pipeline.py` | Airflow DAG + Variable-driven op_kwargs |
| `vertex_pipeline_scheduler.py` | GCS download + `aiplatform.PipelineJob.submit` |
| `gcs_utils.py` | Connection-aware GCS client helpers |
| `BUSINESS_CASE.md` | Why Composer owns submit, not runtime |
| `ARCHITECTURE.md` | Components + Mermaid diagram |
| `DATA_FLOW.md` | Submit path, flag matrix, failure modes |

## Quick start

```bash
python -c "import ast; ast.parse(open('gcs_utils.py').read())"
python -c "import ast; ast.parse(open('vertex_pipeline_scheduler.py').read())"
python -c "import ast; ast.parse(open('dag_vertex_pipeline.py').read())"
```

To run for real you need Vertex AI Pipeline Operator IAM, a GCS
template URI, Airflow Variables for project / SA / step flags, and a
GCP connection (or ADC). This folder is a sanitized reference, not a
deploy package. `PipelineJob.submit` requires `google-cloud-aiplatform`.

## Sanitization notes

- GCP projects `hd-dwh-vertex-prod-*` / `hd-dwh-stream-*` →
  Variable `foodgraph_vertex_project` (default `vertex_ml_project`)
- Service account `trigger-pipelines@…` → Variable
  `foodgraph_vertex_pipeline_sa`
- Template bucket `foodgraph-pipeline-template` →
  `gs://foodgraph-pipeline-templates/…`
- Pipeline root `gs://dev-vertex-foodgraph-model-artifacts` →
  Variable `foodgraph_vertex_pipeline_root`
- Emails / owners → `dataops@example.com` / `data-platform`
- Removed production habit of logging keyfile JSON bodies
- Hardcoded step flags → Airflow Variables (`foodgraph_vertex_*_flag`)
- Package import `horeca_digital.food_graph_vertex*` → local modules
- `PythonOperator` import path modernised; `max_active_runs=1` added
- Kept fire-and-forget `submit()` (Vertex owns runtime) — documented
  tradeoff vs pattern 44 wait-for-exit
- Fixed undefined `connection_json` in the unused `download_blob` path
  by shipping only the helpers the scheduler actually needs

## Category

`ml_pipelines/45-foodgraph-vertex-pipeline-job/`
