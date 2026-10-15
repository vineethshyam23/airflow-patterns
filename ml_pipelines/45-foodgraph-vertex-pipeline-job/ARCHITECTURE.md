# Architecture: Food Graph Vertex PipelineJob

Composer owns schedule, Variables, and submit. Vertex AI owns pipeline
runtime. GCS holds the compiled KFP template. Pattern 40 (separate DAG)
lands outputs after the pipeline writes BigQuery tables.

## Diagram

```mermaid
flowchart TB
  subgraph schedule [Schedule]
    CRON["Cron @daily"]
  end

  subgraph vars [Airflow Variables]
    PROJ["foodgraph_vertex_project"]
    TPL["foodgraph_vertex_pipeline_template"]
    ROOT["foodgraph_vertex_pipeline_root"]
    SA["foodgraph_vertex_pipeline_sa"]
    FLAGS["foodgraph_vertex_*_flag"]
    ISO["foodgraph_vertex_iso_code / env"]
  end

  subgraph compose [Composer DAG etl_vertex_foodgraph_pipeline]
    START[submit_vertex_pipeline]
  end

  subgraph gcs [Cloud Storage]
    TEMPLATE[("foodgraph_training_pipeline.json")]
    ARTIFACTS[("pipeline_root artifacts")]
  end

  subgraph vertex [Vertex AI]
    JOB["PipelineJob.submit"]
    STEPS["Step_1 gold … Step_8 gap analysis"]
  end

  subgraph downstream [Downstream — pattern 40]
    PROP["etl_foodgraph BQ propagation"]
  end

  CRON --> START
  PROJ --> START
  TPL --> START
  ROOT --> START
  SA --> START
  FLAGS --> START
  ISO --> START
  START -->|download template| TEMPLATE
  START -->|PipelineJob| JOB
  JOB --> STEPS
  STEPS --> ARTIFACTS
  STEPS -.->|outputs land later| PROP
```

## Components

**dag_vertex_pipeline.py**  
Builds `op_kwargs` from Variables and the GCP connection extra, then
runs a single `PythonOperator`. `max_active_runs=1`.

**vertex_pipeline_scheduler.py**  
Downloads the template, constructs `aiplatform.PipelineJob` with
`parameter_values` mapped from step flags, calls `submit()`.

**gcs_utils.py**  
Connection-aware GCS client. Accepts key_path or keyfile_dict; falls
back to ADC. Never logs secret material.

## Boundaries

| Concern | Owner |
|---------|-------|
| Schedule + flag matrix | Composer Variables |
| Compiled pipeline graph | GCS template (ML team) |
| Step runtime / GPU / caching | Vertex AI |
| Landing BQ gold / REX copies | Pattern 40 |
| Lead scoring Cloud Run | Pattern 44 |
