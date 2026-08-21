"""
Hash-delta helpers for the Deepideas main-category gaps export.

Today / yesterday staging tables hold the last successful export
snapshot. New or changed _keyhash/_rowhash pairs become the Avro
payload; yesterday rows that no longer match are soft-closed.

Note: _keyhash is wholesale_id only (production contract). Multiple
category rows share a keyhash; _rowhash carries the category grain.
Delta still works because the send filter is
  keyhash new OR (keyhash known AND rowhash new).

Source (read-only):
  dags/horeca_digital/dana_deepideas_query.py  (GapsCategory class)
  dags/etl_dana_deep_ideas_export.py          (wiring pattern)
"""

from __future__ import annotations

import logging

log = logging.getLogger(__name__)

TODAY = "staging.di_gaps_category_export_today"
YESTERDAY = "staging.di_gaps_category_export_yesterday"


def send_data_query() -> str:
    """Delta rows to Avro-encode and POST."""
    query = f"""
SELECT
  wholesale_id,
  CAST(product_main_cat_desc AS STRING) AS product_main_cat_desc,
  CAST(product_main_cat_id AS INT64) AS product_main_cat_id,
  CAST(avg_relevance AS STRING) AS avg_relevance
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
SELECT
  wholesale_id,
  product_main_cat_desc,
  product_main_cat_id,
  avg_relevance,
  _keyhash,
  _rowhash,
  _valid_from,
  _valid_until,
  _valid_flag
FROM `{TODAY}`
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
