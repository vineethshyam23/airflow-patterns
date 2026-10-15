"""
Airflow DAG: multi-table Offer Tool Cloud SQL → BigQuery SCD Type 2.

Pattern: for each of ~15 OLTP tables, Cloud SQL CSV export → copy into
the raw zone → snapshot trusted into a tmp table → load staging →
insert new/changed hashes → expire superseded rows → promote tmp back
to trusted.

Exports are chained sequentially so we do not hammer the Cloud SQL
instance with 15 concurrent dumps. Each table's BigQuery SCD chain
runs independently once its export finishes.

Distinct from pattern 01 (matching-engine SCD on already-landed
results). This DAG owns the OLTP extract + hash-at-source + trusted
historization for a product recommendation / sales tool.

Source (read-only):
  dags/etl_customized_offering.py
  dags/horeca_digital/customized_offering_queries.py
"""

from datetime import date, datetime, timedelta

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.cloud_sql import CloudSQLExportInstanceOperator
from airflow.providers.google.cloud.transfers.bigquery_to_bigquery import BigQueryToBigQueryOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator

import export_queries as queries

default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "start_date": datetime(2021, 1, 1),
    "email": ["dataops@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

dag = DAG(
    dag_id="etl_offer_tool_scd_ingest",
    default_args=default_args,
    schedule_interval="15 6 * * *",
    start_date=default_args["start_date"],
    max_active_runs=1,
    catchup=False,
    tags=["offer-tool", "cloud-sql", "scd2", "trusted"],
    doc_md=(
        "Daily Cloud SQL CSV export of Offer Tool tables into BigQuery "
        "trusted with Type 2 historization (hash change detection)."
    ),
)

# Non-prod config kept as a comment pattern — production runs one config.
# Swap connection / project / suffix when wiring a second environment.
prod_config = {
    "queries": queries.EXPORT_QUERIES,
    "tablenames": queries.TABLE_NAMES,
    "export_bucket": "db-export-offer-tool-prod",
    "connection": "bigquery_default",
    "source_db": "offer_tool_backend_prod",
    "source_project": "offer-tool-prod",
    "source_instance": "db-prod-mysql-offer-tool",
    "raw_bucket": "dwh-rawzone",
    "destination_project": "dwh_project",
    "table_suffix": "_prod",
}

configurations = [prod_config]

dataset = "trusted"
dataset_stg = "trusted_staging"
SOURCE_SYSTEM = "OfferTool"

# GCS object paths use parse-time date/hour. Prefer {{ ds }} / {{ ts }}
# in a rewrite; left as-is here because the production DAG behaved this
# way and path drift only shows up on long-running / re-queued tasks.
last_task = None

for config in configurations:
    for tablename, query in zip(config["tablenames"], config["queries"]):
        suffix = config["table_suffix"]
        bucket_path = "offer-tool/{}/{}/{}/{}{}.csv".format(
            tablename,
            date.today().strftime("%Y-%m-%d"),
            datetime.now().strftime("%H0000"),
            tablename,
            suffix,
        )

        table_stg = "ot_" + tablename + suffix
        tmp_fqn = f"{config['destination_project']}.{dataset_stg}.tmp_{table_stg}"
        stg_fqn = f"{config['destination_project']}.{dataset_stg}.{table_stg}"
        trusted_fqn = f"{config['destination_project']}.{dataset}.{table_stg}"

        export_uri = f"gs://{config['export_bucket']}/{bucket_path}"
        export_body = {
            "exportContext": {
                "uri": export_uri,
                "fileType": "CSV",
                "csvExportOptions": {"selectQuery": query},
                "databases": [config["source_db"]],
            }
        }

        sql_export = CloudSQLExportInstanceOperator(
            body=export_body,
            project_id=config["source_project"],
            instance=config["source_instance"],
            task_id=f"sql_export_{tablename}{suffix}",
            trigger_rule="all_done",
            gcp_conn_id=config["connection"],
            dag=dag,
        )

        copy_file = GCSToGCSOperator(
            task_id=f"cp_file_{tablename}{suffix}",
            source_bucket=config["export_bucket"],
            source_object=bucket_path,
            destination_bucket=config["raw_bucket"],
            destination_object=bucket_path,
            trigger_rule="all_done",
            gcp_conn_id=config["connection"],
            dag=dag,
        )

        # Snapshot current trusted into tmp before we mutate via INSERT/UPDATE.
        copy_tmp = BigQueryToBigQueryOperator(
            task_id=f"copy_table_{tablename}{suffix}_tmp",
            source_project_dataset_tables=trusted_fqn,
            destination_project_dataset_table=tmp_fqn,
            write_disposition="WRITE_TRUNCATE",
            create_disposition="CREATE_IF_NEEDED",
            trigger_rule="all_done",
            gcp_conn_id="bigquery_default",
            dag=dag,
        )

        load_staging = GCSToBigQueryOperator(
            task_id=f"load_offer_tool_{tablename}{suffix}",
            gcp_conn_id="bigquery_default",
            bucket=config["raw_bucket"],
            source_format="CSV",
            source_objects=[bucket_path],
            destination_project_dataset_table=stg_fqn,
            schema_object=f"schema_json/{table_stg}.json",
            create_disposition="CREATE_IF_NEEDED",
            write_disposition="WRITE_TRUNCATE",
            trigger_rule="all_done",
            dag=dag,
        )

        # Append rows whose (key,row) hash pair is new vs current valid OfferTool rows.
        insert_new = BigQueryInsertJobOperator(
            task_id=f"insert_offer_tool_{tablename}{suffix}",
            configuration={
                "query": {
                    "query": (
                        "SELECT *, timestamp(format_timestamp('%Y-%m-%d %H:00:00', current_timestamp)) "
                        "AS _valid_from, "
                        "timestamp('2099-12-31 00:00:00') AS _valid_until, True AS _valid_flag "
                        f"FROM `{stg_fqn}` "
                        "WHERE concat(_keyhash,_rowhash) NOT IN ("
                        f"  SELECT concat(_keyhash,_rowhash) FROM `{trusted_fqn}` "
                        f"  WHERE _valid_flag=True AND _sourcesystem='{SOURCE_SYSTEM}')"
                    ),
                    "useLegacySql": False,
                    "destinationTable": {
                        "projectId": config["destination_project"],
                        "datasetId": tmp_fqn.split(".")[1],
                        "tableId": tmp_fqn.split(".")[2],
                    },
                    "writeDisposition": "WRITE_APPEND",
                    "allowLargeResults": True,
                }
            },
            gcp_conn_id="bigquery_default",
            trigger_rule="all_done",
            dag=dag,
        )

        # Expire rows still marked valid in tmp but absent from today's staging extract.
        expire_old = BigQueryInsertJobOperator(
            task_id=f"update_offer_tool_{tablename}{suffix}",
            configuration={
                "query": {
                    "query": (
                        f"UPDATE `{tmp_fqn}` "
                        "SET _update_ts=current_timestamp, "
                        "_valid_until=timestamp_sub("
                        "  timestamp(format_timestamp('%Y-%m-%d %H:00:00', current_timestamp)), "
                        "  INTERVAL 1 SECOND), "
                        "_valid_flag=False "
                        "WHERE _valid_flag=True "
                        f"AND _sourcesystem='{SOURCE_SYSTEM}' "
                        "AND concat(_keyhash,_rowhash) NOT IN ("
                        f"  SELECT concat(_keyhash,_rowhash) FROM `{stg_fqn}`)"
                    ),
                    "useLegacySql": False,
                }
            },
            gcp_conn_id="bigquery_default",
            trigger_rule="all_done",
            dag=dag,
        )

        promote = BigQueryToBigQueryOperator(
            task_id=f"copy_table_{tablename}{suffix}",
            source_project_dataset_tables=tmp_fqn,
            destination_project_dataset_table=trusted_fqn,
            write_disposition="WRITE_TRUNCATE",
            create_disposition="CREATE_IF_NEEDED",
            trigger_rule="all_done",
            gcp_conn_id="bigquery_default",
            dag=dag,
        )

        # Serialize Cloud SQL exports; fan out BQ chains per table.
        if last_task is not None:
            last_task >> sql_export
        last_task = sql_export

        sql_export >> copy_file >> copy_tmp >> load_staging >> insert_new >> expire_old >> promote
