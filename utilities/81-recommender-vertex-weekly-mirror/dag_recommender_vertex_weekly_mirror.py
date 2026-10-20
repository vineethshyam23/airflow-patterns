"""
Airflow DAG: weekly declarative copy_jobs WRITE_TRUNCATE mirrors.

Flow:
  start
    → one BigQueryInsertJobOperator per copy_jobs entry (parallel)
    → end

Two destination groups share the same registry:
  - Cross-project REX market potential → refined (SELECT *)
  - Intra-DWH insights share queries → insights_share (SELECT / light reshape)

Add another dict to copy_jobs (or an entry in SHARE_QUERIES) to onboard
a new mirror. Do not spin up a second DAG for the same contract.

Source (read-only):
  dags/etl_recommender_vertex.py
  dags/dish_insights_share_queries.py

Distinct from Food Graph ML propagation (#40), Vertex PipelineJob
submit (#45), and POS Intelligence partner export (#53): this is a
weekly BATCH full-replace mirror registry, not training, pipeline
submit, or Avro event-bus publish.
"""

from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

from insights_share_queries import SHARE_QUERIES

try:
    from airflow.operators.empty import EmptyOperator
except ModuleNotFoundError:
    from airflow.operators.dummy import DummyOperator as EmptyOperator  # type: ignore


default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "start_date": datetime(2022, 10, 23),
    "email": ["dataops@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}

environment = "env"
env = os.environ.get(environment, Variable.get(environment))

if env == "DEV":
    projectid = "dwh_project_dev"
    gcp_conn_id = "google_cloud_dev"
else:
    projectid = "dwh_project"
    gcp_conn_id = "google_cloud_default"

# Mondays 11:00 UTC. Weekly freshness is enough for these consumers;
# BATCH priority keeps slot cost down vs INTERACTIVE.
schedule = "0 11 * * 1"

# Fixed cross-project source (REX Vertex market zone). Destination
# flips with env; source stays the authoritative prod market project.
REX_VERTEX_PROJECT = "rex_vertex_project"
REX_MARKET_DATASET = "datazone_metro_pl"

dag = DAG(
    dag_id="etl_recommender_vertex",
    default_args=default_args,
    schedule_interval=schedule,
    max_active_runs=1,
    render_template_as_native_obj=True,
    catchup=False,
    tags=["recommender", "bigquery", "weekly-mirror"],
)

start = EmptyOperator(task_id="start", dag=dag)
end = EmptyOperator(task_id="end", dag=dag)

# One entry per full-replace copy. dataset is the destination dataset
# inside projectid. Queries stay SELECT-only.
copy_jobs = [
    {
        "table": "pol_horeca_shg_potential",
        "dataset": "refined",
        "query": (
            f"SELECT * FROM `{REX_VERTEX_PROJECT}.{REX_MARKET_DATASET}"
            ".horeca_shg_potential`"
        ),
    },
    {
        "table": "pol_article_potential",
        "dataset": "refined",
        "query": (
            f"SELECT * FROM `{REX_VERTEX_PROJECT}.{REX_MARKET_DATASET}"
            ".article_potential`"
        ),
    },
]

for table_name, table_query in SHARE_QUERIES.items():
    copy_jobs.append(
        {
            "table": table_name,
            "dataset": "insights_share",
            "query": table_query,
        }
    )

copy_tasks = []
for job in copy_jobs:
    copy_tasks.append(
        BigQueryInsertJobOperator(
            task_id=f"copy_{job['table']}",
            configuration={
                "query": {
                    "query": job["query"],
                    "useLegacySql": False,
                    "priority": "BATCH",
                    "destinationTable": {
                        "projectId": projectid,
                        "datasetId": job["dataset"],
                        "tableId": job["table"],
                    },
                    "writeDisposition": "WRITE_TRUNCATE",
                    "createDisposition": "CREATE_IF_NEEDED",
                }
            },
            gcp_conn_id=gcp_conn_id,
            dag=dag,
        )
    )

start >> copy_tasks >> end
