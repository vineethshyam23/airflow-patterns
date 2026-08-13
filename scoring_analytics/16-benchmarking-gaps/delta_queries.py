"""
Hash-delta helpers for the peer benchmarking gaps event export.

Today / yesterday staging tables hold the last successful export
snapshot. New or changed _keyhash/_rowhash pairs become the Avro
payload; yesterday rows that no longer match are soft-closed.

Source (read-only):
  dags/horeca_digital/dana_deepideas_query.py  (BenchmarkingGaps class)
  dags/etl_dana_deep_ideas_export.py          (wiring pattern)

The production insert SQL for "today" is market-specific and large; this
module keeps the reusable delta contract. Wire your own insert SELECT
(or a view over refined.benchmarking_gaps_*) into staging.di_benchmarking_gaps_export_today
before calling send_data_query().
"""

from __future__ import annotations

import logging

log = logging.getLogger(__name__)

TODAY = "staging.di_benchmarking_gaps_export_today"
YESTERDAY = "staging.di_benchmarking_gaps_export_yesterday"


def send_data_query() -> str:
    """Delta rows to Avro-encode and POST."""
    query = f"""
SELECT
  wholesale_id,
  CAST(segment AS STRING) AS segment,
  CAST(cust_segment_revenue AS STRING) AS cust_segment_revenue,
  CAST(avg_segment_revenue AS STRING) AS avg_segment_revenue,
  CAST(main_cat_id AS INT64) AS main_cat_id,
  CAST(main_cat_desc AS STRING) AS main_cat_desc,
  CAST(cust_cat_revenue AS STRING) AS cust_cat_revenue,
  CAST(avg_segment_cat_revenue AS STRING) AS avg_segment_cat_revenue,
  CAST(avg_cust_article_price AS STRING) AS avg_cust_article_price,
  CAST(avg_segment_article_price AS STRING) AS avg_segment_article_price
FROM `{TODAY}`
WHERE
  _keyhash NOT IN (SELECT _keyhash FROM `{YESTERDAY}`)
  OR (
    _keyhash IN (SELECT _keyhash FROM `{YESTERDAY}`)
    AND _rowhash NOT IN (SELECT _rowhash FROM `{YESTERDAY}`)
  )
"""
    log.info("Retrieved query: send_data_query")
    return query


def copy_yesterday_query() -> str:
    """Append today's delta onto the yesterday snapshot table."""
    query = f"""
SELECT * FROM `{TODAY}`
WHERE
  _keyhash NOT IN (SELECT _keyhash FROM `{YESTERDAY}`)
  OR (
    _keyhash IN (SELECT _keyhash FROM `{YESTERDAY}`)
    AND _rowhash NOT IN (SELECT _rowhash FROM `{YESTERDAY}`)
  )
"""
    log.info("Retrieved query: copy_yesterday_query")
    return query


def update_yesterday_query() -> str:
    """Soft-close yesterday rows superseded by today's hash change."""
    query = f"""
UPDATE `{YESTERDAY}`
SET
  _valid_until = TIMESTAMP(
    FORMAT_TIMESTAMP(
      "%Y-%m-%d 23:59:59",
      TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
    )
  ),
  _valid_flag = FALSE
WHERE
  _valid_flag = TRUE
  AND _keyhash IN (SELECT _keyhash FROM `{TODAY}`)
  AND CONCAT(_keyhash, _rowhash) NOT IN (
    SELECT CONCAT(_keyhash, _rowhash) FROM `{TODAY}`
  )
"""
    log.info("Retrieved query: update_yesterday_query")
    return query


if __name__ == "__main__":
    for name, fn in (
        ("send", send_data_query),
        ("copy", copy_yesterday_query),
        ("update", update_yesterday_query),
    ):
        sql = fn()
        print(name, "chars", len(sql), "has_today", TODAY in sql)
