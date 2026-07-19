"""
Salesforce asset history SQL builders.

Builds BigQuery statements that:
  1. Snapshot CRM asset/install rows from a refined SFDC asset history table
  2. Emit _keyhash / _rowhash for SCD-style delta detection
  3. Produce today insert, hist append, hist expire, and send SELECTs

Key identity is establishment_UID + account identifier — one establishment
can have multiple assets, but the business key used in production was the
establishment + CRM account pair. Rowhash covers the full outbound payload.

Source (read-only): dags/horeca_digital/dana_sfdc_asset_query.py
"""

import logging


class AssetQuery:
    # ISO-2 → ISO-3 for country-partitioned source tables.
    # This export started as a single-market (ES) pilot; the map stays so a
    # second country can reuse the same builders without rewriting SQL.
    countries = {
        "es": "esp",
    }

    @staticmethod
    def get_asset_export_insert_query():
        """
        WRITE_TRUNCATE query for trusted_staging.sfdc_asset_history_today.

        Source table is a refined SFDC asset history extract (install /
        disable / channel / referrer attributes plus shipping + account ids).
        """
        query = """
WITH asset_history AS (
  SELECT *
  FROM `refined.sfdc_asset_history_ES`
)
SELECT
  CAST(establishment_UID AS STRING) AS establishment_UID,
  CAST(Product AS STRING) AS Product,
  CAST(ChannelV2 AS STRING) AS ChannelV2,
  CAST(Referrer AS STRING) AS Referrer,
  CAST(ActivatedBy AS STRING) AS ActivatedBy,
  CAST(InstallDate AS DATE) AS InstallDate,
  CAST(DisabledDate AS DATE) AS DisabledDate,
  CAST(Status AS STRING) AS Status,
  CAST(Establishment_name AS STRING) AS Establishment_name,
  CAST(ShippingPostalCode AS STRING) AS ShippingPostalCode,
  CAST(ShippingCity AS STRING) AS ShippingCity,
  CAST(ShippingStreet AS STRING) AS ShippingStreet,
  CAST(PersonEmail AS STRING) AS PersonEmail,
  CAST(email_permission AS BOOL) AS email_permission,
  CAST(AccountId_Long AS STRING) AS AccountId_Long,
  CAST(Crm_Metro_Id AS STRING) AS Crm_Metro_Id,
  CAST(Store_Id AS INT64) AS Store_Id,
  CAST(Crm_Account_Identifier AS INT64) AS Crm_Account_Identifier,
  CAST(VAT_id AS STRING) AS VAT_id,
  CAST(metro_id AS INT64) AS metro_id,
  CAST(cust_no AS INT64) AS cust_no,
  CAST(home_store_id AS INT64) AS home_store_id,
  CAST(_create_ts AS STRING) AS _create_ts,
  CAST(_update_ts AS STRING) AS _update_ts,
  CURRENT_DATE() AS _ldts,
  TO_HEX(MD5(CONCAT(
    IFNULL(CAST(establishment_UID AS STRING), ''), '|',
    IFNULL(CAST(Crm_Account_Identifier AS STRING), '')
  ))) AS _keyhash,
  TO_HEX(MD5(CONCAT(
    IFNULL(CAST(establishment_UID AS STRING), ''), '|',
    IFNULL(CAST(Product AS STRING), ''), '|',
    IFNULL(CAST(ChannelV2 AS STRING), ''), '|',
    IFNULL(CAST(Referrer AS STRING), ''), '|',
    IFNULL(CAST(ActivatedBy AS STRING), ''), '|',
    IFNULL(CAST(InstallDate AS STRING), ''), '|',
    IFNULL(CAST(DisabledDate AS STRING), ''), '|',
    IFNULL(CAST(Status AS STRING), ''), '|',
    IFNULL(CAST(Establishment_name AS STRING), ''), '|',
    IFNULL(CAST(ShippingPostalCode AS STRING), ''), '|',
    IFNULL(CAST(ShippingCity AS STRING), ''), '|',
    IFNULL(CAST(ShippingStreet AS STRING), ''), '|',
    IFNULL(CAST(PersonEmail AS STRING), ''), '|',
    IFNULL(CAST(email_permission AS STRING), ''), '|',
    IFNULL(CAST(AccountId_Long AS STRING), ''), '|',
    IFNULL(CAST(Crm_Metro_Id AS STRING), ''), '|',
    IFNULL(CAST(Store_Id AS STRING), ''), '|',
    IFNULL(CAST(Crm_Account_Identifier AS STRING), ''), '|',
    IFNULL(CAST(VAT_id AS STRING), ''), '|',
    IFNULL(CAST(metro_id AS STRING), ''), '|',
    IFNULL(CAST(cust_no AS STRING), ''), '|',
    IFNULL(CAST(home_store_id AS STRING), ''), '|',
    IFNULL(CAST(_create_ts AS STRING), ''), '|',
    IFNULL(CAST(_update_ts AS STRING), '')
  ))) AS _rowhash,
  CURRENT_TIMESTAMP() AS _valid_from,
  TIMESTAMP('2099-12-31 23:59:59') AS _valid_until,
  TRUE AS _valid_flag
FROM asset_history
"""
        return query

    @staticmethod
    def get_asset_export_hist_query(today_table, hist_table):
        """
        Delta SELECT: new keys or changed rowhash vs hist.

        Used for WRITE_APPEND into the hist table after ingest succeeds.
        """
        query = f"""
SELECT
  establishment_UID,
  Product,
  ChannelV2,
  Referrer,
  ActivatedBy,
  InstallDate,
  DisabledDate,
  Status,
  Establishment_name,
  ShippingPostalCode,
  ShippingCity,
  ShippingStreet,
  PersonEmail,
  email_permission,
  AccountId_Long,
  Crm_Metro_Id,
  Store_Id,
  Crm_Account_Identifier,
  VAT_id,
  metro_id,
  cust_no,
  home_store_id,
  _create_ts,
  _update_ts,
  _ldts,
  _keyhash,
  _rowhash,
  _valid_from,
  _valid_until,
  _valid_flag
FROM `trusted_staging.{today_table}`
WHERE
  _keyhash NOT IN (SELECT _keyhash FROM `trusted_staging.{hist_table}`)
  OR (
    _keyhash IN (SELECT _keyhash FROM `trusted_staging.{hist_table}`)
    AND _rowhash NOT IN (SELECT _rowhash FROM `trusted_staging.{hist_table}`)
  )
"""
        logging.info("Retrieved query: get_asset_export_hist_query")
        return query

    @staticmethod
    def get_asset_export_update_query(today_table, hist_table):
        """
        Expire active hist rows superseded by today's snapshot.

        Closes _valid_until at end of yesterday and clears _valid_flag.
        """
        query = f"""
UPDATE `trusted_staging.{hist_table}`
SET
  _valid_until = TIMESTAMP(
    FORMAT_TIMESTAMP(
      '%Y-%m-%d 23:59:59',
      TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
    )
  ),
  _valid_flag = FALSE
WHERE
  _valid_flag = TRUE
  AND _keyhash IN (SELECT _keyhash FROM `trusted_staging.{today_table}`)
  AND CONCAT(_keyhash, _rowhash) NOT IN (
    SELECT CONCAT(_keyhash, _rowhash) FROM `trusted_staging.{today_table}`
  )
"""
        logging.info("Retrieved query: get_asset_export_update_query")
        return query

    @staticmethod
    def get_asset_export_send_query(today_table, hist_table):
        """
        Outbound delta for the event API (same predicate as hist append).

        Hist must still reflect *previous* run state when this runs —
        otherwise the send SELECT returns empty and the bus goes quiet.
        """
        query = f"""
SELECT
  establishment_UID,
  Product,
  ChannelV2,
  Referrer,
  ActivatedBy,
  InstallDate,
  DisabledDate,
  Status,
  Establishment_name,
  ShippingPostalCode,
  ShippingCity,
  ShippingStreet,
  PersonEmail,
  email_permission,
  AccountId_Long,
  Crm_Metro_Id,
  Store_Id,
  Crm_Account_Identifier,
  VAT_id,
  metro_id,
  cust_no,
  home_store_id,
  _create_ts,
  _update_ts,
  _ldts
FROM `trusted_staging.{today_table}`
WHERE
  _keyhash NOT IN (SELECT _keyhash FROM `trusted_staging.{hist_table}`)
  OR (
    _keyhash IN (SELECT _keyhash FROM `trusted_staging.{hist_table}`)
    AND _rowhash NOT IN (SELECT _rowhash FROM `trusted_staging.{hist_table}`)
  )
"""
        logging.info("Retrieved query: get_asset_export_send_query")
        return query


if __name__ == "__main__":
    logging.basicConfig(
        format="%(asctime)s %(levelname)-4s %(message)s",
        level=logging.INFO,
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    today = "sfdc_asset_history_today"
    hist = "sfdc_asset_history_hist"
    q = AssetQuery()
    print("-- insert --")
    print(q.get_asset_export_insert_query()[:400], "...")
    print("-- send --")
    print(q.get_asset_export_send_query(today, hist)[:300], "...")
