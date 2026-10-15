"""Submit a Vertex AI PipelineJob from a GCS-hosted KFP template.

Composer downloads the compiled pipeline JSON, then submits a
``PipelineJob`` with per-step boolean flags. That lets ops enable gold
ingredient processing, gap analysis, recipe extraction, etc. without
recompiling the Kubeflow graph.

Distinct from pattern 40 (BQ gold + cross-project copy of ML outputs)
and pattern 44 (Cloud Run Execute Job for lead scoring). This pattern
owns *training / processing pipeline submission* on Vertex AI.

Source (read-only):
  dags/horeca_digital/food_graph_vertex.py
  dags/horeca_digital/food_graph_vertex_utils.py
"""

from __future__ import annotations

import argparse
import os
from datetime import datetime

from google.cloud import aiplatform

from gcs_utils import CloudStorageUtil


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Food Graph Vertex pipeline scheduler")
    parser.add_argument("--output_dir", type=str, default="output")
    parser.add_argument(
        "--source",
        type=str,
        default="gs://foodgraph-pipeline-templates/foodgraph_training_pipeline.json",
    )
    parser.add_argument("--project_id", type=str, default="vertex_ml_project")
    parser.add_argument(
        "--service_account",
        type=str,
        default="trigger-pipelines@vertex_ml_project.iam.gserviceaccount.com",
    )
    parser.add_argument("--location", type=str, default="europe-west3")
    parser.add_argument("--pipeline_root", type=str, default="gs://vertex-foodgraph-artifacts")
    parser.add_argument("--display_name", type=str, default="foodgraph-training-pipeline")
    parser.add_argument("--ISO_CODE", type=str, default="DE")
    parser.add_argument("--env", type=str, default="acc")
    parser.add_argument("--default_true_flag", type=str, default="True")
    parser.add_argument("--articles_to_ing_flag", type=str, default="False")
    parser.add_argument("--data_preprocessor_flag", type=str, default="False")
    parser.add_argument("--gold_ingredient_processor_flag", type=str, default="True")
    parser.add_argument("--gap_analysis_flag", type=str, default="False")
    parser.add_argument("--ingredient_normaliser_flag", type=str, default="False")
    parser.add_argument("--menu_items_to_recipe_flag", type=str, default="False")
    parser.add_argument("--recipes_to_ing_flag", type=str, default="False")
    parser.add_argument("--purchase_frequency_flag", type=str, default="False")
    parser.add_argument("--manual_gaps_flag", type=str, default="False")
    parser.add_argument("--enable_persist_results", type=str, default="False")
    parser.add_argument("--caching_flag", type=str, default="False")
    parser.add_argument(
        "--connection_json",
        type=str,
        default="",
        help="Unused on CLI; Airflow passes a dict via op_kwargs",
    )
    return parser.parse_args(argv)


def _as_bool(value) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def pipeline_scheduler(**kwargs):
    """Airflow PythonOperator entrypoint (also usable from CLI via main)."""
    output_dir = kwargs.get("output_dir", "output")
    input_source = kwargs["source"]
    project_id = kwargs.get("project_id", "vertex_ml_project")
    service_account = kwargs.get(
        "service_account",
        "trigger-pipelines@vertex_ml_project.iam.gserviceaccount.com",
    )
    location = kwargs.get("location", "europe-west3")
    pipeline_root = kwargs.get("pipeline_root", "gs://vertex-foodgraph-artifacts")
    display_name = kwargs.get("display_name", "foodgraph-training-pipeline")
    iso_code = kwargs.get("ISO_CODE", "DE")
    env = kwargs.get("env", "acc")
    default_true_flag = kwargs.get("default_true_flag", "True")
    enable_persist_results = _as_bool(kwargs.get("enable_persist_results", False))
    caching_flag = _as_bool(kwargs.get("caching_flag", False))
    connection_json = kwargs.get("connection_json") or {}

    os.makedirs(output_dir, exist_ok=True)
    destination_file = os.path.join(output_dir, "foodgraph_training_pipeline.json")

    CloudStorageUtil.download_to_location(
        project=project_id,
        source_file=input_source,
        destination_file_name=destination_file,
        connection_json=connection_json,
    )

    # Map Airflow kwargs → Vertex pipeline parameter_values.
    # String "True"/"False" matches the compiled KFP template contract.
    parameter_values = {
        "DEFAULT_TRUE_FLAG": default_true_flag,
        "Step_4_articles_to_ing_flag": kwargs.get("articles_to_ing_flag", "False"),
        "Step_2_data_preprocessor_flag": kwargs.get("data_preprocessor_flag", "False"),
        "Step_1_gold_ingredient_processor_flag": kwargs.get(
            "gold_ingredient_processor_flag", "True"
        ),
        "Step_8_gap_analysis_flag": kwargs.get("gap_analysis_flag", "False"),
        "Step_3_ingredient_normaliser_flag": kwargs.get("ingredient_normaliser_flag", "False"),
        "Step_6_menu_items_to_recipe_flag": kwargs.get("menu_items_to_recipe_flag", "False"),
        "Step_5_recipes_to_ing_flag": kwargs.get("recipes_to_ing_flag", "False"),
        "Step_7_purchase_frequency_flag": kwargs.get("purchase_frequency_flag", "False"),
        "ENV": env,
        "ISO_CODE": iso_code,
        "ENABLE_PERSIST_RESULTS": enable_persist_results,
    }

    job_id = f"foodgraph-pipeline-{datetime.utcnow().strftime('%Y-%m-%d-%H-%M-%S')}"

    run = aiplatform.PipelineJob(
        project=project_id,
        enable_caching=caching_flag,
        location=location,
        display_name=display_name,
        template_path=destination_file,
        job_id=job_id,
        pipeline_root=pipeline_root,
        parameter_values=parameter_values,
    )
    run.submit(service_account=service_account)
    return {"job_id": job_id, "display_name": display_name, "parameter_values": parameter_values}


if __name__ == "__main__":
    args = vars(parse_args())
    # CLI has no Airflow connection; empty dict → ADC.
    args["connection_json"] = {}
    pipeline_scheduler(**args)
