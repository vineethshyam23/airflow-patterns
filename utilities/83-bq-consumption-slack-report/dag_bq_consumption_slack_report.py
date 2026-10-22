"""
Airflow DAG: daily BigQuery user-consumption Slack report.

Queries an aggregated monitoring table for:
  1. Yesterday's top-3 human users by GiB scanned (+ GB/query ratio)
  2. Heaviest user overall (label in Slack says "30 day"; SQL is all-time)
  3. Current calendar-month totals (GiB + cost)

Then posts one Slack webhook message. Service accounts are excluded
from the human leaderboard so bot traffic does not drown the signal.

Source (read-only):
  dags/bq_consumption_statistics_slack.py

Distinct from pattern 80 (Slack on materialization failure aggregate):
this is a scheduled cost-awareness digest, not a failure reporter.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

try:
    from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
except ImportError:  # older Composer images
    from airflow.contrib.hooks.bigquery_hook import BigQueryHook  # type: ignore

# --- sanitized identifiers -------------------------------------------------
MONITORING_TABLE = "monitoring.bq_usage"
SLACK_CONN_ID = "slack_webhook_conn"
SLACK_CHANNEL = "#dwh-ops"
BQ_LOCATION = "EU"
DAG_ID = "bq_user_consumption_statistics"

YESTERDAY_TOP_SQL = f"""
WITH t1 AS (
  SELECT
    username,
    total_billed_gibibytes,
    total_costs_in_eur,
    n_queries,
    ROUND(total_billed_gibibytes / n_queries, 2) AS ratio,
    ROW_NUMBER() OVER () AS rn
  FROM `{MONITORING_TABLE}`
  WHERE query_date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    AND username NOT LIKE '%gserviceaccount%'
  ORDER BY total_billed_gibibytes DESC
  LIMIT 3
)
SELECT
  username,
  total_billed_gibibytes,
  n_queries,
  ratio,
  CASE
    WHEN ratio < 2 THEN ':slightly_smiling_face:'
    WHEN ratio BETWEEN 2 AND 5 THEN ':face_with_raised_eyebrow:'
    WHEN ratio BETWEEN 5 AND 10 THEN ':white_frowning_face:'
    WHEN ratio > 15 THEN ':cop:'
    WHEN ratio > 10 THEN ':weary:'
  END AS efficiency_emoji,
  CASE
    WHEN rn = 1 THEN ':first_place_medal:'
    WHEN rn = 2 THEN ':second_place_medal:'
    WHEN rn = 3 THEN ':third_place_medal:'
  END AS rank_emoji,
  CASE
    WHEN rn = 1 AND total_billed_gibibytes < 500 THEN 'a staggering'
    WHEN rn = 1 AND total_billed_gibibytes BETWEEN 500 AND 1000
      THEN 'an _*extremely excessive*_'
    WHEN rn = 1 AND total_billed_gibibytes > 1000
      THEN 'a _*WAY TOO HEAVY*_ :rotating_light:'
    WHEN rn = 2 THEN 'a whopping'
    WHEN rn = 3 THEN 'a modest'
  END AS descriptor,
  total_costs_in_eur
FROM t1
"""

# Production quirk: Slack text says "30 day record" but there is no
# date filter — this is the all-time heaviest human user.
HEAVIEST_USER_SQL = f"""
SELECT
  username,
  ROUND(SUM(total_billed_gibibytes), 2),
  SUM(n_queries),
  ROUND(SUM(total_costs_in_eur), 2)
FROM `{MONITORING_TABLE}`
WHERE username NOT LIKE '%gserviceaccount%'
GROUP BY 1
ORDER BY 2 DESC
LIMIT 1
"""

MONTH_TOTAL_SQL = f"""
SELECT
  ROUND(SUM(total_billed_gibibytes)),
  ROUND(SUM(total_costs_in_eur), 2)
FROM `{MONITORING_TABLE}`
WHERE EXTRACT(MONTH FROM query_date) = EXTRACT(MONTH FROM CURRENT_DATE())
  AND EXTRACT(YEAR FROM query_date) = EXTRACT(YEAR FROM CURRENT_DATE())
"""


def get_status(**_context):
    """Build the Slack digest and post it via webhook.

    Production note: SlackWebhookOperator is created and `.execute()`d
    inside this callable. That bypasses Airflow task tracking for the
    Slack step — keep as shipped, or split into XCom + dedicated task
    when you modernize.
    """
    bqh = BigQueryHook(use_legacy_sql=False, location=BQ_LOCATION)

    lines = ["Info on yesterday's BigQuery statistics:"]
    for row in bqh.get_records(YESTERDAY_TOP_SQL):
        # row: user, gib, n_queries, ratio, eff_emoji, rank_emoji, desc, cost
        lines.append(
            f"{row[5]} {row[0]} with {row[6]} consumption of {row[1]} GB "
            f"data scanned costing {row[7]} Euro considering {row[2]} queries. "
            f"Ratio: {row[3]} {row[4]}"
        )

    for row in bqh.get_records(HEAVIEST_USER_SQL):
        lines.append(
            f"30 day record goes to {row[0]} :trophy: with {row[1]} GB "
            f"scanned costing {row[3]} Euro over {row[2]} queries"
        )

    for row in bqh.get_records(MONTH_TOTAL_SQL):
        lines.append(
            f":information_source: Total usage for current month: "
            f"{row[0]} GB data scanned costing {row[1]} Euro"
        )

    message = "\n".join(lines) + "\n"

    # Connection password is fetched in production; webhook conn id is
    # what SlackWebhookOperator actually uses.
    _ = BaseHook.get_connection(SLACK_CONN_ID).password

    alert = SlackWebhookOperator(
        task_id="slack_notification",
        slack_webhook_conn_id=SLACK_CONN_ID,
        message=message,
        channel=SLACK_CHANNEL,
        username="airflow",
        dag=dag,
    )
    alert.execute({})


default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "start_date": datetime(2024, 2, 14),
    "email": ["dataops@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=10),
}

dag = DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    schedule_interval="30 6 * * *",
    catchup=False,
    tags=["monitoring", "bigquery", "cost-awareness"],
)

PythonOperator(
    task_id="get_status",
    python_callable=get_status,
    do_xcom_push=False,
    trigger_rule="all_done",
    dag=dag,
)
