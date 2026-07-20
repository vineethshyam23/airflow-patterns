"""
SQL for the daily Odoo helpdesk tickets event export.

Yesterday's Level-1 tickets only — create_date delta, not a full reload.
The refined table is assumed to be refreshed by the upstream dbt Cloud job
before this query runs.

Source (read-only): inline SQL in dags/etl_dana_odoo_helpdesk_tickets.py
"""


def get_helpdesk_tickets_send_query(refined_table: str = "refined.odoo_helpdesk_ticket") -> str:
    """
    Select yesterday's tickets for Avro bulk ingest.

    Filters:
      - ticket_number IS NOT NULL (drop incomplete rows)
      - DATE(create_date) = CURRENT_DATE() - 1 (one-day incremental)
    """
    return f"""
SELECT
  ticket_number,
  ticket_name,
  CAST(create_date AS STRING) AS create_date,
  ticket_type,
  ticket_tag,
  CAST(close_date AS STRING) AS close_date,
  country,
  escalated_check,
  current_status,
  ticket_medium,
  account_identifier,
  customer_id,
  store_id
FROM `{refined_table}`
WHERE ticket_number IS NOT NULL
  AND DATE(create_date) = CURRENT_DATE() - 1
""".strip()


if __name__ == "__main__":
    print(get_helpdesk_tickets_send_query())
