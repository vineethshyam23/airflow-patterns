"""SQL builder: CRM partner-ID cleaning for FR/RO matching engine.

Takes messy free-text wholesale partner IDs from CRM establishments,
matches them against ~10 regex patterns per country, reconstructs a
canonical numeric ID, and validates it against the trusted wholesale
customer table. Output is WRITE_TRUNCATE-friendly
(iso_code, UID__c, clean_partner_id).

Source (read-only):
  dags/horeca_digital/archived/etl_refined_zone_monthly.py
  (task: sfdc_establishment_clean_metro_id)
"""

from __future__ import annotations

REFINED = "refined"
TRUSTED_WHOLESALE = "trusted_wholesale"
TRUSTED_VIEWS = "trusted_views"
CRM_ESTABLISHMENT = "crm_establishment"
DEST_TABLE = "crm_establishment_clean_partner_id"


def partner_id_clean_sql(project: str) -> str:
    """Rebuild cleaned partner IDs for FR and RO establishments."""
    cust_fr = f"`{project}.{TRUSTED_WHOLESALE}.fra_wholesale_customer`"
    cust_ro = f"`{project}.{TRUSTED_WHOLESALE}.rom_wholesale_customer`"
    crm = f"`{project}.{TRUSTED_VIEWS}.{CRM_ESTABLISHMENT}`"

    return f"""
-- CRM partner-ID cleaning for matching engine (FR / RO)
WITH wholesale_customers AS (
  SELECT DISTINCT
    iso_code,
    partner_id,
    unique_partner_id,
    CAST(home_store_id AS STRING) AS home_store_id,
    CAST(cust_no AS STRING) AS cust_no
  FROM (
    (SELECT 'FR' AS iso_code, partner_id, unique_partner_id,
            home_store_id, cust_no
     FROM {cust_fr})
    UNION ALL
    (SELECT 'RO' AS iso_code, partner_id, unique_partner_id,
            home_store_id, cust_no
     FROM {cust_ro})
  )
),

crm_establishments AS (
  SELECT DISTINCT
    a.ShippingCountryCode AS iso_code,
    a.UID__c,
    TRIM(a.Partner_Id__c) AS Partner_Id__c,
    a.Store__c
  FROM {crm} a
  WHERE a.ShippingCountryCode IN ('FR', 'RO')
    AND a.UID__c IS NOT NULL
    AND a.Partner_Id__c IS NOT NULL
    AND a.Partner_Id__c != 'Platform Days'
    AND REGEXP_CONTAINS(a.Partner_Id__c, r'[0-9]')
    AND NOT REGEXP_CONTAINS(a.Partner_Id__c, r'^0*$')
),

-- Immediate validation lookup for store+customer split patterns
valid_customers AS (
  SELECT DISTINCT
    iso_code,
    CAST(home_store_id AS STRING) AS home_store_id,
    CAST(cust_no AS STRING) AS cust_no
  FROM wholesale_customers
),

patterns AS (
  SELECT iso_code, t.pattern
  FROM UNNEST(
    ARRAY<STRUCT<pattern STRING, iso_code ARRAY<STRING>>>[
      ('[0-9]{{5,8}}$ + Store__c', ['FR']),
      ('^([0-9]{{3,7}})$ + Store__c', ['FR']),
      ('home_store_id2 + cust_no', ['FR', 'RO']),
      ('home_store_id3 + cust_no', ['FR', 'RO']),
      ('^22([0-9]{{9,10}})$', ['FR']),
      ('^22([0-9]{{11,13}})$', ['FR']),
      ('^250(?:01|77)[0-9]{{16,17}}$|^25001[0-9]{{11}}$', ['FR']),
      ('^(?:22)?[0-9]{{1,3}} ?[ -\\\\/] ?[0-9]{{1,8}}$', ['FR']),
      ('^25077[0-9]{{11}}$', ['FR']),
      ('^[0-9]{{14}}$', ['RO'])
    ]
  ) t
  JOIN UNNEST(t.iso_code) iso_code
),

-- Public ISO numeric country prefixes (FR=25001, RO=64201)
country_code AS (
  SELECT 'FR' AS iso_code, '25001' AS code UNION ALL
  SELECT 'RO' AS iso_code, '64201' AS code
),

found_pattern_combinations AS (
  SELECT DISTINCT
    b.iso_code, b.UID__c, b.Partner_Id__c, b.Store__c, p.pattern
  FROM crm_establishments b
  JOIN patterns p USING (iso_code)
  WHERE REGEXP_CONTAINS(b.Partner_Id__c, p.pattern) IS TRUE
     OR (
       pattern = '^([0-9]{{3,7}})$ + Store__c'
       AND REGEXP_CONTAINS(Partner_Id__c, r'^[0-9]{{3,7}}$')
       AND Store__c IS NOT NULL
     )
     OR (
       pattern = '[0-9]{{6}}$ + Store__c'
       AND REGEXP_CONTAINS(
         Partner_Id__c,
         CONCAT('^', LPAD(Store__c, 3, '0'), r'[0-9]{{6}}$')
       )
       AND Store__c IS NOT NULL
     )
     OR (
       pattern = '[0-9]{{5,8}}$ + Store__c'
       AND REGEXP_CONTAINS(
         Partner_Id__c,
         CONCAT('^', Store__c, r'[0-9]{{5,8}}$')
       )
       AND Store__c IS NOT NULL
     )
     OR (
       pattern = 'home_store_id3 + cust_no'
       AND STRUCT(
         LTRIM(SUBSTR(Partner_Id__c, 1, 3), '0'),
         LTRIM(SUBSTR(Partner_Id__c, 4), '0')
       ) IN (SELECT STRUCT(home_store_id, cust_no) FROM valid_customers)
     )
     OR (
       pattern = 'home_store_id2 + cust_no'
       AND STRUCT(
         LTRIM(SUBSTR(Partner_Id__c, 1, 2), '0'),
         LTRIM(SUBSTR(Partner_Id__c, 3), '0')
       ) IN (SELECT STRUCT(home_store_id, cust_no) FROM valid_customers)
     )
),

cleaning AS (
  SELECT
    *,
    CASE
      WHEN pattern LIKE '%cust_no + Store__c' THEN
        CAST(
          CONCAT(
            code,
            LPAD(extracted.home_store_id, 3, '0'),
            LPAD(extracted.cust_no, 8, '0')
          ) AS INT64
        )
      WHEN pattern LIKE '%Store__c' THEN
        CAST(
          CONCAT(
            code,
            LPAD(extracted.home_store_id, 3, '0'),
            LPAD(REGEXP_EXTRACT(Partner_Id__c, extracted.cust_no), 8, '0')
          ) AS INT64
        )
      ELSE
        CAST(
          CONCAT(
            code,
            LPAD(
              REGEXP_EXTRACT(Partner_Id__c, extracted.home_store_id),
              3,
              '0'
            ),
            LPAD(
              REGEXP_EXTRACT(Partner_Id__c, extracted.cust_no),
              8,
              '0'
            )
          ) AS INT64
        )
    END AS clean_partner_id
  FROM (
    SELECT
      *,
      CASE pattern
        WHEN r'^([0-9]{{3,7}})$ + Store__c' THEN
          STRUCT(Store__c AS home_store_id, r'^([0-9]{{3,7}})$' AS cust_no)
        WHEN r'^(?:22)?[0-9]{{1,3}} ?[ -/] ?[0-9]{{1,8}}$' THEN
          STRUCT(
            r'^(?:22)?([0-9]{{1,3}}) ?[ -/]' AS home_store_id,
            r'[ -/] ?([0-9]{{0,8}})$' AS cust_no
          )
        WHEN r'^22([0-9]{{11,13}})$' THEN
          STRUCT(
            r'^22([0-9]{{3}})' AS home_store_id,
            r'^22[0-9]{{3}}([0-9]*)[0-9]{{2}}$' AS cust_no
          )
        WHEN r'^22([0-9]{{9,10}})$' THEN
          STRUCT(
            r'^22([0-9]{{3}})' AS home_store_id,
            r'^22[0-9]{{3}}([0-9]{{6,7}})$' AS cust_no
          )
        WHEN r'[0-9]{{6}}$ + Store__c' THEN
          STRUCT(Store__c AS home_store_id, r'[0-9]{{6}}$' AS cust_no)
        WHEN r'[0-9]{{5,8}}$ + Store__c' THEN
          STRUCT(
            Store__c AS home_store_id,
            CONCAT('^', Store__c, r'([0-9]{{5,8}})$') AS cust_no
          )
        WHEN 'home_store_id3 + cust_no' THEN
          STRUCT(
            LTRIM(SUBSTR(Partner_Id__c, 1, 3), '0') AS home_store_id,
            LTRIM(SUBSTR(Partner_Id__c, 4), '0') AS cust_no
          )
        WHEN 'home_store_id2 + cust_no' THEN
          STRUCT(
            LTRIM(SUBSTR(Partner_Id__c, 1, 2), '0') AS home_store_id,
            LTRIM(SUBSTR(Partner_Id__c, 3), '0') AS cust_no
          )
        WHEN r'cust_no + home_store_id' THEN
          STRUCT(Store__c AS home_store_id, Partner_Id__c AS cust_no)
        WHEN r'^250(?:01|77)[0-9]{{16,17}}$|^25001[0-9]{{11}}$' THEN
          STRUCT(
            r'^250(?:01|77)([0-9]{{3}})' AS home_store_id,
            r'^250(?:01|77)[0-9]{{3}}([0-9]{{8}})' AS cust_no
          )
        WHEN r'^0(padded_home_store_id)[0-9]{{5}}$' THEN
          STRUCT(
            r'^0([0-9]{{3}})' AS home_store_id,
            r'([0-9]{{5}})$' AS cust_no
          )
        WHEN r'^[0-9]{{3}} [0-9]{{8}} [0-9]{{6}}$' THEN
          STRUCT(
            r'^([0-9]{{3}})' AS home_store_id,
            r'^[0-9]{{3}} ([0-9]{{8}})' AS cust_no
          )
        WHEN r'^[0-9]{{14}}$' THEN
          STRUCT(
            r'^([0-9]{{3}})' AS home_store_id,
            r'^[0-9]{{3}}([0-9]{{8}})' AS cust_no
          )
      END AS extracted
    FROM found_pattern_combinations
  )
  JOIN country_code USING (iso_code)
),

-- Drop rows where reconstructed store disagrees with Store__c
cleaned_with_valid_store AS (
  SELECT * EXCEPT (Store__c)
  FROM cleaning
  WHERE Store__c IS NULL
     OR LPAD(SUBSTR(CAST(clean_partner_id AS STRING), 6, 3), 3, '0')
        = LPAD(CAST(Store__c AS STRING), 3, '0')
),

valid_pattern_combinations AS (
  SELECT
    c.iso_code,
    c.UID__c,
    c.Partner_Id__c,
    c.clean_partner_id,
    IFNULL(valid_partner_customer, FALSE) AS is_valid
  FROM cleaned_with_valid_store c
  LEFT JOIN (
    SELECT DISTINCT
      iso_code,
      partner_id,
      unique_partner_id,
      TRUE AS valid_partner_customer
    FROM wholesale_customers
  ) mc
    ON clean_partner_id = partner_id
   AND c.iso_code = mc.iso_code
),

cleaned_partner_id AS (
  SELECT
    res.iso_code,
    res.UID__c,
    res.clean_partner_id
  FROM valid_pattern_combinations res
  WHERE res.is_valid IS TRUE
)

SELECT
  iso_code,
  UID__c,
  clean_partner_id
FROM cleaned_partner_id
ORDER BY iso_code
"""


if __name__ == "__main__":
    sql = partner_id_clean_sql("dwh_project")
    assert "clean_partner_id" in sql
    assert "crm_establishment" in sql
    assert "Metro_Id" not in sql
    assert "sfdc" not in sql.lower()
    assert "hd-dwh" not in sql
    print("ok: partner-id cleaning SQL builder")
