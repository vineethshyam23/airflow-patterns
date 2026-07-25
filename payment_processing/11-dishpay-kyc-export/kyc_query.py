"""
SQL for payment-product KYC export to a partner event bus.

The refined table is already filtered (country + product) in dbt.
This module only shapes columns for the Avro contract — timestamps
and dates as strings, integers left as-is for Avro long.

Source (read-only):
  dags/horeca_digital/dana_dishpay_kyc_query.py
"""

from __future__ import annotations

import logging


class PaymentKyc:
    # Pilot market when this shipped. Country / product filters live in
    # dbt staging — expanding here without a matching schema registration
    # just dumps empty result sets into the bus.
    countries = ["pl"]

    @staticmethod
    def get_send_query() -> str:
        """
        SELECT shaped for Avro string/long fields.

        Unqualified project — BigQuery client project (DEV vs PROD)
        resolves the dataset. Do not hard-code project in this SQL.
        """
        query = """
        SELECT
            establishment_id,
            country_code,
            FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', kyc_created_dt) AS kyc_created_dt,
            CAST(kyc_modified_dt AS STRING) AS kyc_modified_dt,
            kyc_step,
            kyc_step_details,
            kyc_status,
            kyc_duration_day,
            kyc_duration_month,
            kyc_onboarding_successful,
            kyc_first_attempt,
            kyc_adyen_pending_validation,
            kyc_adyen_error_validation
        FROM `refined.payment_kyc_export`
        """
        logging.getLogger().info("Retrieved query: get_send_query (payment_kyc)")
        return query


if __name__ == "__main__":
    print(PaymentKyc.get_send_query().strip()[:120], "...")
