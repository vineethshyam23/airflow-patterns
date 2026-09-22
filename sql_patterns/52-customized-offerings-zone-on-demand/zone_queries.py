"""Offer Tool on-demand zone query builders (sanitized).

Lean subset used by ``dag_offer_tool_zone_on_demand.py`` for manual
product-zone refreshes: Elasticsearch search index, ingredient images,
and soft-delete filters on wholesale card mappings.

Sibling of pattern 48 ``zone_queries.py`` (scheduled weekday fan-out with
gaps / assortment / scores). This module keeps only what the on-demand
DAG actually publishes.

Source (read-only): dags/horeca_digital/customized_offering_queries.py
"""

from __future__ import annotations

DWH_PROJECT = "dwh_project"


def _dwh() -> str:
    return DWH_PROJECT


def elasticsearch_data_query(stage: str, iso_code: str) -> str:
    """Build the search-index projection for one country × stage.

    Product stages other than ``dev`` still read trusted / refined from
    the ``acc`` Food Graph cut — same staging convention as the scheduled
    zone. The ``stage`` argument is retained so a future cutover to
    non-dev sources is a one-line change (see TODO in source).
    """
    # Staging cut reserved for a future non-dev source cutover (source TODO).
    # Today the projection always reads DWH refined; ``stage`` still selects
    # which product project receives the WRITE_TRUNCATE.
    _ = "dev" if stage == "dev" else "acc"

    return f"""
WITH
  all_establishments AS (
    SELECT DISTINCT
      est.iso_code,
      est.establishment_name,
      est.street_name,
      est.street_number,
      est.postal_code,
      est.city,
      est.establishment_id,
      est.google_places_id AS gpid,
      est.wholesale_id,
      est.geo_long,
      est.geo_lat,
      est.vat_id,
      est.active_cust,
      est.sfdc_establishment_id,
      est.google_places_id,
      rb_group_customer_potential,
      branch_desc_customer_potential,
      wholesale.establishment_name AS wholesale_name
    FROM `{DWH_PROJECT}.refined.all_establishments_{iso_code}` est
    LEFT JOIN (
      SELECT
        establishment_id,
        MAX(rb_group_customer_potential) AS rb_group_customer_potential,
        MAX(branch_desc_customer_potential) AS branch_desc_customer_potential
      FROM (
        SELECT
          establishment_id,
          CASE
            WHEN segment_column = 'rb_group' THEN customer_potential_category
            ELSE NULL
          END AS rb_group_customer_potential,
          CASE
            WHEN segment_column = 'branch_desc' THEN customer_potential_category
            ELSE NULL
          END AS branch_desc_customer_potential
        FROM `{DWH_PROJECT}.refined.benchmarking_gaps_{iso_code}`
      )
      GROUP BY establishment_id
    ) bg
      ON est.establishment_id = bg.establishment_id
    LEFT JOIN (
      SELECT wholesale_id, establishment_name
      FROM `{DWH_PROJECT}.refined.all_wholesale_establishments_{iso_code}`
    ) wholesale
      ON est.wholesale_id = wholesale.wholesale_id
    WHERE offer_tool_relevant
      AND data_source = 'all'
  )
SELECT
  t1.iso_code,
  t1.establishment_name,
  t1.street_name,
  t1.street_number,
  t1.postal_code,
  t1.city,
  t1.establishment_id AS eid,
  rb_group_customer_potential,
  branch_desc_customer_potential,
  t1.google_places_id AS gpid,
  t1.wholesale_id AS wholesale_id,
  t1.geo_long AS long,
  t1.geo_lat AS lat,
  t1.vat_id,
  t1.active_cust,
  NULL AS region_data,
  NULL AS categories_data,
  NULL AS category_labels,
  NULL AS last_viewed,
  wholesale_name
FROM all_establishments t1
"""


def ingredients_images_query() -> str:
    """Gold ingredient name ↔ image URL join for the Offer Tool catalog."""
    return f"""
SELECT
  a.name AS gold_ingredient,
  a.proper_name AS gold_ingredient_corrected,
  b.image_url AS gold_ingredient_image_url,
  TRUE AS source_valid_flag,
  TRUE AS _valid_flag,
  a.iso_code
FROM `{DWH_PROJECT}.trusted.fg_ingredients_translations` AS a
INNER JOIN `{DWH_PROJECT}.trusted.fg_ingredients` AS b
  ON a.ing_id = b.ing_id
"""


def exclude_deleted_statement(*, field: str, iso_code: str = "*") -> str:
    """Filter out soft-deleted wholesale cards from mapping publishes.

    ``is_deleted`` lives on ``wholesale_id``. For ``unique_wholesale_id``
    we only exclude when *every* underlying card is deleted.
    """
    if field == "wholesale_id":
        return f"""({field} IS NULL
                            OR  SAFE_CAST({field} AS INT64) NOT IN
                                (
                                    SELECT {field}
                                    FROM `{DWH_PROJECT}.refined.analytical_wholesale_customers_{iso_code}`
                                    WHERE is_deleted = 1
                                ))"""
    if field == "unique_wholesale_id":
        return f"""({field} IS NULL
                            OR  SAFE_CAST({field} AS INT64) NOT IN
                                (
                                    SELECT {field}
                                    FROM `{DWH_PROJECT}.refined.analytical_wholesale_customers_{iso_code}`
                                    GROUP BY {field}
                                    HAVING sum(is_deleted) = count(*)
                                ))"""
    raise ValueError(f"Unsupported input: {field}")
