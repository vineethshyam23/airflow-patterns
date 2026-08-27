"""
Airflow DAG: single-market platform Order + Reservation monthly export.

One dbt Cloud job refreshes both refined tables, then two Python tasks
full-load Orders and Reservations to the partner event bus in parallel
(fan-out after dbt, both join end with ALL_DONE).

Source (read-only):
  dags/etl_dana_pl_dish_orders_reservations_export.py
  dags/horeca_digital/dana_pl_dish_orders_export.py
  dags/horeca_digital/dana_pl_dish_orders_query.py

Distinct from pattern 23 (multi-country product footprint) and
pattern 24 (MAG acquisition/penetration aggregates). This feed is a
lifetime transactional extract for one market's Order and Reservation
products, joined to the latest subscription asset attributes.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable

from orders_export import (
    send_market_dish_orders_data,
    send_market_dish_reservations_data,
)
from orders_query import MarketDishOrders

default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "email": ["dataops@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "dbt_cloud_conn_id": "dbt_conn",
    "account_id": Variable.get("dbt_cloud_account_id", default_var=1),
}

# Monthly on the 1st at 06:00 UTC — after month-open refine of the
# market Order / Reservation tables.
schedule = "0 6 1 * *"
_JOB_TIMEOUT = 600

# dbt Cloud job that materializes both refined tables under one tag.
DBT_JOB_ID = int(Variable.get("dbt_market_dish_orders_job_id", default_var="0"))

dag = DAG(
    dag_id="etl_market_dish_orders_reservations_export",
    default_args=default_args,
    schedule_interval=schedule,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    tags=["market", "dish-order", "dish-reservation", "dbt", "event-ingest", "monthly"],
    doc_md=(
        "Monthly full-load of single-market platform Order + Reservation "
        "rows to the partner event bus after one dbt refresh."
    ),
)

start = EmptyOperator(task_id="start", dag=dag)
end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_DONE, dag=dag)

# check_interval kept as a fixed fraction of timeout so Composer does
# not burn dbt Cloud status polls on a 10-minute job.
dbt_refresh = DbtCloudRunJobOperator(
    task_id="dbt_market_dish_orders_refresh",
    job_id=DBT_JOB_ID,
    check_interval=max(30, _JOB_TIMEOUT // 10),
    dag=dag,
    do_xcom_push=False,
    timeout=_JOB_TIMEOUT,
)

start >> dbt_refresh

for country in MarketDishOrders.countries:
    export_orders = PythonOperator(
        task_id=f"export_market_dish_orders_{country.upper()}",
        python_callable=send_market_dish_orders_data,
        op_kwargs={"country": country},
        execution_timeout=timedelta(hours=4),
        dag=dag,
    )
    export_reservations = PythonOperator(
        task_id=f"export_market_dish_reservations_{country.upper()}",
        python_callable=send_market_dish_reservations_data,
        op_kwargs={"country": country},
        execution_timeout=timedelta(hours=4),
        dag=dag,
    )
    dbt_refresh >> export_orders >> end
    dbt_refresh >> export_reservations >> end
