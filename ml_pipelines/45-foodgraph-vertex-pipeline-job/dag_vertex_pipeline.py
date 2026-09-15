"""Food Graph Vertex AI PipelineJob scheduler DAG.

Daily (or on-demand) Composer job that:

1. Resolves GCP project / template URI / step flags from Variables.
2. Downloads the compiled KFP pipeline JSON from GCS.
3. Submits a Vertex AI ``PipelineJob`` with per-step enable flags.
4. Returns immediately after submit (Vertex owns runtime; Composer
   owns schedule + parameter contract).

Source (read-only):
  dags/horeca_digital/archived/etl_food_graph_vertex.py
  dags/horeca_digital/food_graph_vertex.py
  dags/horeca_digital/food_graph_vertex_utils.py

Distinct from pattern 40 (propagates Vertex *outputs* across BQ
projects) and pattern 44 (waits on Cloud Run Execute Job). This DAG
submits the training / processing pipeline itself.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from vertex_pipeline_scheduler import pipeline_scheduler

VERTEX_PROJECT = Variable.get("foodgraph_vertex_project", default_var="vertex_ml_project")
PIPELINE_TEMPLATE = Variable.get(
    "foodgraph_vertex_pipeline_template",
    default_var="gs://foodgraph-pipeline-templates/foodgraph_training_pipeline.json",
)
PIPELINE_ROOT = Variable.get(
    "foodgraph_vertex_pipeline_root",
    default_var="gs://vertex-foodgraph-artifacts",
)
SERVICE_ACCOUNT = Variable.get(
    "foodgraph_vertex_pipeline_sa",
    default_var="trigger-pipelines@vertex_ml_project.iam.gserviceaccount.com",
)
LOCATION = Variable.get("foodgraph_vertex_location", default_var="europe-west3")
ISO_CODE = Variable.get("foodgraph_vertex_iso_code", default_var="DE")
ENV = Variable.get("foodgraph_vertex_env", default_var="acc")
GCP_CONN_ID = Variable.get("foodgraph_vertex_gcp_conn_id", default_var="bigquery_default")

# Per-step flags — keep as Variables so ops can flip a stage without
# editing the DAG. Defaults match the production acceptance schedule
# (gold ingredient processor on; heavier stages off).
STEP_DEFAULTS = {
    "default_true_flag": "True",
    "articles_to_ing_flag": "False",
    "data_preprocessor_flag": "False",
    "gold_ingredient_processor_flag": "True",
    "gap_analysis_flag": "False",
    "ingredient_normaliser_flag": "False",
    "menu_items_to_recipe_flag": "False",
    "recipes_to_ing_flag": "False",
    "purchase_frequency_flag": "False",
    "manual_gaps_flag": "False",
}


def _flag(name: str) -> str:
    return Variable.get(f"foodgraph_vertex_{name}", default_var=STEP_DEFAULTS[name])


def _connection_extra() -> dict:
    try:
        return BaseHook.get_connection(GCP_CONN_ID).extra_dejson or {}
    except Exception:
        # Reference / parse-only environments without the connection.
        return {}


default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "start_date": datetime(2022, 1, 1),
    "email": ["dataops@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

dag = DAG(
    dag_id="etl_vertex_foodgraph_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    max_active_runs=1,
    catchup=False,
    tags=["etl", "foodgraph", "ml", "vertex", "pipeline"],
    doc_md=__doc__,
)

op_kwargs = {
    "source": PIPELINE_TEMPLATE,
    "project_id": VERTEX_PROJECT,
    "service_account": SERVICE_ACCOUNT,
    "location": LOCATION,
    "pipeline_root": PIPELINE_ROOT,
    "display_name": "foodgraph-training-pipeline",
    "ISO_CODE": ISO_CODE,
    "env": ENV,
    "output_dir": "/tmp/foodgraph_vertex_pipeline",
    "enable_persist_results": Variable.get(
        "foodgraph_vertex_enable_persist_results", default_var="False"
    ),
    "caching_flag": Variable.get("foodgraph_vertex_caching_flag", default_var="False"),
    "connection_json": _connection_extra(),
}
for flag_name in STEP_DEFAULTS:
    op_kwargs[flag_name] = _flag(flag_name)

submit_pipeline = PythonOperator(
    task_id="submit_vertex_pipeline",
    python_callable=pipeline_scheduler,
    op_kwargs=op_kwargs,
    dag=dag,
)

submit_pipeline
