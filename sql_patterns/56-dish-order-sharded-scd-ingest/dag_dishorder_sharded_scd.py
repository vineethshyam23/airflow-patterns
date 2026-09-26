"""
Airflow DAG: multi-shard food-ordering Cloud SQL → BigQuery SCD Type 2.

Engineering shape:
  1. Discover tenant→shard mapping from the master MySQL
  2. Fan out parallel CSV exports across shards (plus master dims)
  3. Merge shard fragments into one raw-zone object per table
  4. Per table: snapshot trusted → load staging → insert new hashes →
     expire superseded rows → promote tmp back to trusted

Distinct from pattern 27 (single Offer Tool instance, sequential
CloudSQLExportInstanceOperator dumps). Distinct from pattern 39
(Hydra weekly full dump, no SCD) and pattern 54 (Reservation Tool
id-watermark + Sunday full sync, historization in dbt).

Source (read-only):
  dags/etl_dishorder.py
"""

from datetime import date, datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.transfers.bigquery_to_bigquery import (
    BigQueryToBigQueryOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.utils.trigger_rule import TriggerRule

from bq_reservation import ReservedBigQueryInsertJobOperator
from shard_config import (
    ALL_TABLES,
    DATA_ROOT,
    EXPORT_PREFIX,
    MASTER_TABLES,
    SCRIPTS_ROOT,
    SHARD_INSTANCES,
    SOURCE_SYSTEM,
    TABLE_PREFIX,
    is_sharded,
    max_bad_records,
)

default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "start_date": datetime(2021, 1, 26),
    "email": ["dataops@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

dag = DAG(
    dag_id="etl_foodorder_sharded_scd",
    default_args=default_args,
    schedule_interval="30 0 * * *",
    max_active_runs=1,
    catchup=False,
    tags=["food-order", "cloud-sql", "scd2", "sharded", "trusted"],
    doc_md=(
        "Daily multi-shard Cloud SQL CSV land of food-ordering tables "
        "into BigQuery trusted with Type 2 historization (hash change detection)."
    ),
)

# Sanitized config — production used concrete project / bucket names.
export_bucket = "db-export-food-order-prod"
raw_bucket = "dwh-rawzone"
destination_project = "dwh_project"
dataset = "trusted"
dataset_staging = "trusted_staging"
gcp_conn_id = "bigquery_default"

# -------------------------------------------------------------------------
# Phase 1 — export + merge
# -------------------------------------------------------------------------

getdbs_all = BashOperator(
    task_id="getdbs",
    bash_command=f"{SCRIPTS_ROOT}/getdbs.sh ",
    trigger_rule=TriggerRule.ALL_SUCCESS,
    dag=dag,
)

wait_for_loads = EmptyOperator(
    task_id="wait_for_files",
    dag=dag,
    trigger_rule=TriggerRule.ALL_DONE,
)

merge_files = BashOperator(
    task_id="mergefiles",
    bash_command=f"{SCRIPTS_ROOT}/dishordermerge.sh ",
    trigger_rule=TriggerRule.ALL_SUCCESS,
    dag=dag,
)

clean_files = BashOperator(
    task_id="clean_files",
    bash_command=f"{SCRIPTS_ROOT}/clean.sh ",
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag,
)

export_master = BashOperator(
    task_id="export_foodorder_master",
    bash_command=f"{SCRIPTS_ROOT}/dishorder.sh ",
    trigger_rule=TriggerRule.ALL_SUCCESS,
    dag=dag,
)

for instance, ip in sorted(SHARD_INSTANCES.items()):
    # Filter the global tenant map down to DBs living on this shard IP.
    filter_dbs = BashOperator(
        task_id=f"getdbs_{instance}",
        bash_command=(
            f"grep -E '{ip}\"' {DATA_ROOT}/foodorder_dbs.csv "
            f"| awk -F ',' '{{print $1}}' "
            f"> {DATA_ROOT}/foodorder_dbs_{instance}.csv"
        ),
        trigger_rule=TriggerRule.ALL_SUCCESS,
        dag=dag,
    )

    export_shard = BashOperator(
        task_id=f"export_foodorder_shard_{instance}",
        bash_command=f"{SCRIPTS_ROOT}/dishordersplitted.sh {instance} {ip}",
        trigger_rule=TriggerRule.ALL_SUCCESS,
        dag=dag,
    )

    getdbs_all >> filter_dbs >> export_shard >> export_master

export_master >> merge_files

# -------------------------------------------------------------------------
# Phase 2 — per-table SCD Type 2 load chains (parallel after merge)
# -------------------------------------------------------------------------

# Parse-time date in the load path matches production behaviour. Prefer
# {{ ds }} for backfill-safe rewrites; left as-is so path drift on long
# runs stays visible as a known tradeoff (see BUSINESS_CASE.md).
run_date = date.today().strftime("%Y-%m-%d")

for tablename in ALL_TABLES:
    source_object = f"{EXPORT_PREFIX}/{tablename}/{run_date}/{tablename}.csv"
    staging_table = f"{TABLE_PREFIX}{tablename}"
    tmp_fqn = f"{destination_project}.{dataset_staging}.tmp_{staging_table}"
    stg_fqn = f"{destination_project}.{dataset_staging}.{staging_table}"
    trusted_fqn = f"{destination_project}.{dataset}.{staging_table}"

    if is_sharded(tablename):
        # Merge already wrote the raw-zone object — nothing to copy.
        stage_file = EmptyOperator(
            task_id=f"download_file_{tablename}",
            trigger_rule=TriggerRule.ALL_SUCCESS,
            dag=dag,
        )
    else:
        # Master tables land in the export bucket; copy into raw zone.
        stage_file = GCSToGCSOperator(
            task_id=f"download_file_{tablename}",
            gcp_conn_id=gcp_conn_id,
            source_bucket=export_bucket,
            source_object=f"{{{{ tomorrow_ds }}}}/{tablename}.csv",
            destination_bucket=raw_bucket,
            destination_object=(
                f"{EXPORT_PREFIX}/{tablename}/{{{{ tomorrow_ds }}}}/{tablename}.csv"
            ),
            trigger_rule=TriggerRule.ALL_SUCCESS,
            dag=dag,
        )

    snapshot_tmp = BigQueryToBigQueryOperator(
        task_id=f"copy_table_{tablename}_tmp",
        source_project_dataset_tables=trusted_fqn,
        destination_project_dataset_table=tmp_fqn,
        write_disposition="WRITE_TRUNCATE",
        create_disposition="CREATE_IF_NEEDED",
        gcp_conn_id=gcp_conn_id,
        trigger_rule=TriggerRule.ALL_SUCCESS,
        dag=dag,
    )

    load_staging = GCSToBigQueryOperator(
        task_id=f"load_foodorder_{tablename}",
        gcp_conn_id=gcp_conn_id,
        bucket=raw_bucket,
        source_format="CSV",
        source_objects=[source_object],
        destination_project_dataset_table=stg_fqn,
        schema_object=f"schema_json/{staging_table}.json",
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE",
        allow_quoted_newlines=True,
        ignore_unknown_values=True,
        allow_jagged_rows=True,
        field_delimiter=",",
        max_bad_records=max_bad_records(tablename),
        quote_character='"',
        trigger_rule=TriggerRule.ALL_SUCCESS,
        dag=dag,
    )

    # Append rows whose (key, row) hash pair is new vs currently valid rows.
    insert_new = ReservedBigQueryInsertJobOperator(
        task_id=f"insert_foodorder_{tablename}",
        configuration={
            "query": {
                "query": (
                    "SELECT *, "
                    "timestamp(format_timestamp('%Y-%m-%d %H:00:00', current_timestamp)) "
                    "AS _valid_from, "
                    "timestamp('2099-12-31 00:00:00') AS _valid_until, "
                    "True AS _valid_flag "
                    f"FROM `{stg_fqn}` "
                    "WHERE concat(_keyhash, _rowhash) NOT IN ("
                    f"  SELECT concat(_keyhash, _rowhash) FROM `{trusted_fqn}` "
                    f"  WHERE _valid_flag = True AND _sourcesystem = '{SOURCE_SYSTEM}'"
                    ")"
                ),
                "useLegacySql": False,
                "writeDisposition": "WRITE_APPEND",
                "allowLargeResults": True,
                "destinationTable": {
                    "projectId": destination_project,
                    "datasetId": dataset_staging,
                    "tableId": f"tmp_{staging_table}",
                },
            }
        },
        gcp_conn_id=gcp_conn_id,
        trigger_rule=TriggerRule.ALL_SUCCESS,
        dag=dag,
    )

    # Expire tmp rows still valid but absent from today's staging extract.
    expire_old = ReservedBigQueryInsertJobOperator(
        task_id=f"update_foodorder_{tablename}",
        configuration={
            "query": {
                "query": (
                    f"UPDATE `{tmp_fqn}` "
                    "SET _update_ts = current_timestamp, "
                    "_valid_until = timestamp_sub("
                    "  timestamp(format_timestamp('%Y-%m-%d %H:00:00', current_timestamp)), "
                    "  INTERVAL 1 SECOND), "
                    "_valid_flag = False "
                    "WHERE _valid_flag = True "
                    f"AND _sourcesystem = '{SOURCE_SYSTEM}' "
                    "AND concat(_keyhash, _rowhash) NOT IN ("
                    f"  SELECT concat(_keyhash, _rowhash) FROM `{stg_fqn}`"
                    ")"
                ),
                "useLegacySql": False,
            }
        },
        gcp_conn_id=gcp_conn_id,
        trigger_rule=TriggerRule.ALL_SUCCESS,
        dag=dag,
    )

    promote = ReservedBigQueryInsertJobOperator(
        task_id=f"copy_table_{tablename}",
        configuration={
            "query": {
                "query": f"SELECT * FROM `{tmp_fqn}`",
                "destinationTable": {
                    "projectId": destination_project,
                    "datasetId": dataset,
                    "tableId": staging_table,
                },
                "createDisposition": "CREATE_IF_NEEDED",
                "writeDisposition": "WRITE_TRUNCATE",
                "useLegacySql": False,
                "allowLargeResults": True,
            }
        },
        gcp_conn_id=gcp_conn_id,
        trigger_rule=TriggerRule.ALL_SUCCESS,
        dag=dag,
    )

    (
        merge_files
        >> stage_file
        >> snapshot_tmp
        >> load_staging
        >> insert_new
        >> expire_old
        >> promote
        >> wait_for_loads
    )

wait_for_loads >> clean_files

# Keep master table names importable for tests / docs without pulling Airflow.
__all__ = ["dag", "MASTER_TABLES", "SHARD_INSTANCES"]
