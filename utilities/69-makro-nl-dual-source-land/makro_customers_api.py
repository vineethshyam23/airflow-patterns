"""Wholesale NL customer + market land helpers (inbound only).

OAuth2 password-grant client for a partner MCC customer API, paginated
JSON land to the Composer data volume, merge-request extract, and CHD
landing-zone CSV clean/validate used by the ShortCircuit branch.

HubSpot / reverse-export helpers intentionally omitted — those ship with
the outbound pattern. This module is the inbound half only.

Source (read-only):
  dags/horeca_digital/makro_customers_api.py
"""

from __future__ import annotations

import base64
import csv
import io
import json
import logging
import os
import re

import requests
from airflow.models import Variable
from google.cloud import storage

logger = logging.getLogger(__name__)

environment = "env"
env = os.environ.get(environment, Variable.get(environment, default_var="DEV"))

if env == "DEV":
    USERNAME = Variable.get("wholesale_nl_mcc_user", default_var="")
    PASSWORD = Variable.get("wholesale_nl_mcc_password", default_var="")
    CLIENT_ID = Variable.get("mcc_oauth_client_id_dev", default_var="")
    CLIENT_SECRET = Variable.get("mcc_oauth_client_secret_dev", default_var="")
    OAUTH2_URL = Variable.get(
        "mcc_oauth2_url_sbx", default_var="https://mcc.example.com/oauth/token"
    )
    BASE_URL = Variable.get(
        "wholesale_nl_customer_url_sbx",
        default_var="https://mcc.example.com/api/customers",
    )
    BASE_URL_GET_MERGEREQUESTS = Variable.get(
        "wholesale_nl_merge_requests_url_sbx",
        default_var="https://mcc.example.com/api/merge-requests",
    )
    CHD_LANDING_BUCKET = Variable.get(
        "chd_landing_bucket_dev", default_var="landingzone-chd-dev"
    )
else:
    USERNAME = Variable.get("wholesale_nl_mcc_user", default_var="")
    PASSWORD = Variable.get("wholesale_nl_mcc_password", default_var="")
    CLIENT_ID = Variable.get("mcc_oauth_client_id", default_var="")
    CLIENT_SECRET = Variable.get("mcc_oauth_client_secret", default_var="")
    OAUTH2_URL = Variable.get(
        "mcc_oauth2_url", default_var="https://mcc.example.com/oauth/token"
    )
    BASE_URL = Variable.get(
        "wholesale_nl_customer_url",
        default_var="https://mcc.example.com/api/customers",
    )
    BASE_URL_GET_MERGEREQUESTS = Variable.get(
        "wholesale_nl_merge_requests_url",
        default_var="https://mcc.example.com/api/merge-requests",
    )
    CHD_LANDING_BUCKET = Variable.get(
        "chd_landing_bucket", default_var="landingzone-chd"
    )

COMPOSER_DATA_DIR = Variable.get(
    "wholesale_nl_composer_data_dir",
    default_var="/home/airflow/gcs/data/wholesale/customer_NL/",
)
CHD_FILE_PREFIX = Variable.get("chd_file_prefix", default_var="WHOLESALE_NL")
BATCH_SIZE = int(Variable.get("wholesale_nl_api_batch_size", default_var="10000"))


def base64_encode_string(s: str) -> str:
    return base64.b64encode(s.encode()).decode()


class callAPI:
    """Password-grant OAuth2 client with 401 re-auth on GET/POST."""

    def __init__(self, token_url, client_id, client_secret):
        self.token_url = token_url
        self.token = None
        self.client_id = client_id
        self.client_secret = client_secret

    def get_token(self):
        headers = {
            "Authorization": "Basic "
            + base64_encode_string(self.client_id + ":" + self.client_secret),
            "Content-Type": "application/x-www-form-urlencoded",
        }
        params = {
            "grant_type": "password",
            "username": USERNAME,
            "password": PASSWORD,
        }
        r = requests.post(self.token_url, headers=headers, params=params, timeout=60)
        r.raise_for_status()
        self.token = r.json()["access_token"]
        return self.token

    def endpoint_get(self, url):
        if self.token is None:
            self.get_token()
        headers = {"Authorization": "Bearer " + self.token}
        r = requests.get(url, headers=headers, timeout=120)
        if r.status_code == 401:
            self.token = None
            return self.endpoint_get(url)
        r.raise_for_status()
        return r.json()

    def endpoint_post(self, url, data=None):
        if self.token is None:
            self.get_token()
        headers = {
            "Authorization": "Bearer " + self.token,
            "Content-Type": "application/json",
        }
        r = requests.post(url, headers=headers, data=data, timeout=120)
        if r.status_code == 401:
            self.token = None
            return self.endpoint_post(url, data)
        r.raise_for_status()
        try:
            return r.json()
        except json.JSONDecodeError as e:
            raise RuntimeError(
                f"Invalid JSON from partner API (status={r.status_code}): {e}"
            ) from e


def getdata(run_date: str) -> str:
    """Paginated customer-base pull since run_date → Composer JSONL file."""
    logger.info("Starting customer-base fetch for date=%s", run_date)
    mcc = callAPI(OAUTH2_URL, CLIENT_ID, CLIENT_SECRET)
    os.makedirs(COMPOSER_DATA_DIR, exist_ok=True)

    offset = 0
    total_records = 0
    out_path = os.path.join(COMPOSER_DATA_DIR, f"customer_base_{run_date}.json")

    with open(out_path, "w", encoding="utf-8") as outfile:
        while True:
            url = (
                f"{BASE_URL}?last_mutday_from={run_date}"
                f"&offset={offset}&rows={BATCH_SIZE}"
            )
            logger.info("GET customer base offset=%s", offset)
            response = mcc.endpoint_get(url)

            if isinstance(response, str):
                raise RuntimeError(f"Partner API error: {response}")
            if not isinstance(response, dict) or "result" not in response:
                raise RuntimeError(f"Unexpected response shape: {type(response)}")

            records = response["result"]
            if not records:
                break

            for row in records:
                outfile.write(json.dumps(row) + "\n")

            total_records += len(records)
            if len(records) < BATCH_SIZE:
                break
            offset += BATCH_SIZE

    logger.info("Customer-base fetch done. total=%s path=%s", total_records, out_path)
    return "Success"


def getdata_merge_requests(run_date: str) -> str:
    """Full merge-request extract → Composer JSONL (no date filter on API)."""
    mcc = callAPI(OAUTH2_URL, CLIENT_ID, CLIENT_SECRET)
    os.makedirs(COMPOSER_DATA_DIR, exist_ok=True)
    out_path = os.path.join(COMPOSER_DATA_DIR, f"merge_requests_{run_date}.json")

    with open(out_path, "w", encoding="utf-8") as outfile:
        response = mcc.endpoint_get(BASE_URL_GET_MERGEREQUESTS)
        for row in response.get("result") or []:
            tdata = {
                "id_winning": row.get("id_winning"),
                "id_losing": row.get("id_losing"),
                "processed": row.get("processed"),
                "status": row.get("status"),
                "result": row.get("result") if row.get("result") else None,
            }
            outfile.write(json.dumps(tdata) + "\n")
    logger.info("Merge-request extract written to %s", out_path)
    return "Success"


def is_valid_integer(value) -> bool:
    if value is None:
        return False
    value_str = str(value).strip()
    if not value_str or value_str.upper() in {"NULL", "N/A", "NA", "NAN", "NONE"}:
        return False
    try:
        int(value_str)
        return True
    except (ValueError, OverflowError):
        return False


schema_fields = [

  {
    "name": "DS_ID",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "DS_URL",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "BUSINESS_NAME",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "LEGAL_NAME",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "ADDRESS_1",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "ADDRESS_2",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "ADDRESS_3",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "POSTAL_CODE_1",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "LOCALITY_1",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "ADMINISTRATIVE_AREA_1",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "ADMINISTRATIVE_AREA_2",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "ADMINISTRATIVE_AREA_3",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "ADMINISTRATIVE_AREA_4",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "ALTERNATIVE_AREA_1",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "ALTERNATIVE_AREA_2",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "COUNTRY",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "TAGS_PLACES",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "LATITUDE",
    "mode": "NULLABLE",
    "type": "FLOAT",
    "description": "",
    "fields": []
  },
  {
    "name": "LONGITUDE",
    "mode": "NULLABLE",
    "type": "FLOAT",
    "description": "",
    "fields": []
  },
  {
    "name": "PHONE",
    "mode": "NULLABLE",
    "type": "INTEGER",
    "description": "",
    "fields": []
  },
  {
    "name": "WEBSITE",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "CHANNEL",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "SEGMENT_CD",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "SUB_CHANNEL",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "SEGMENT_DESC",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "ENTITY_TYPE",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "CONFIDENCE_LEVEL",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "OPERATOR_STATUS",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "CHAIN_SYSTEM_ID",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "CHAIN_SYSTEM_NAME",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "CHAIN_SIZE_RANGE_CD",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "CHAIN_SIZE_RANGE_DESC",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "MCO_ID",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "MCO_NAME",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "HOST_ID",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "HOST_NAME",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "HOST_NUMBER_OF_UNITS_LOCATED_WITHIN",
    "mode": "NULLABLE",
    "type": "INTEGER",
    "description": "",
    "fields": []
  },
  {
    "name": "CLOSED_TEMPORARILY",
    "mode": "NULLABLE",
    "type": "BOOLEAN",
    "description": "",
    "fields": []
  },
  {
    "name": "OPERATING_HOURS",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "HOURS_MONDAY",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "HOURS_TUESDAY",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "HOURS_WEDNESDAY",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "HOURS_THURSDAY",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "HOURS_FRIDAY",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "HOURS_SATURDAY",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "HOURS_SUNDAY",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "ID_BUSINESS_DETAILED",
    "mode": "NULLABLE",
    "type": "INTEGER",
    "description": "",
    "fields": []
  },
  {
    "name": "ID_BUSINESS_GENERAL",
    "mode": "NULLABLE",
    "type": "INTEGER",
    "description": "",
    "fields": []
  },
  {
    "name": "YEARS_IN_BUSINESS_RANGE_CD",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "YEARS_IN_BUSINESS_RANGE_DESC",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "NUMBER_OF_EMPLOYEES_RANGE_CD",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "NUMBER_OF_EMPLOYEES_RANGE_DESC",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "CUISINE_CD",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "CUISINE_PRIMARY_DESC",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "CUISINE_SECONDARY_DESC",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "TAGS_FOOD",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "EST_ANNUAL_SALES_RANGE_CD",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "EST_ANNUAL_SALES_RANGE_DESC",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "EST_MEALS_PER_DAY_RANGE_CD",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "EST_MEALS_PER_DAY_RANGE_DESC",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "AVERAGE_CHECK_RANGE_CD",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "AVERAGE_CHECK_RANGE_DESC",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "NUMBER_OF_ROOMS_RANGE_CD",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "NUMBER_OF_ROOMS_RANGE_DESC",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "LODGING_STAR_LEVEL",
    "mode": "NULLABLE",
    "type": "INTEGER",
    "description": "",
    "fields": []
  },
  {
    "name": "HAS_RESTAURANT",
    "mode": "NULLABLE",
    "type": "BOOLEAN",
    "description": "",
    "fields": []
  },
  {
    "name": "OFFERS_DELIVERY",
    "mode": "NULLABLE",
    "type": "BOOLEAN",
    "description": "",
    "fields": []
  },
  {
    "name": "OFFERS_DINE_IN",
    "mode": "NULLABLE",
    "type": "BOOLEAN",
    "description": "",
    "fields": []
  },
  {
    "name": "OFFERS_TAKEOUT",
    "mode": "NULLABLE",
    "type": "BOOLEAN",
    "description": "",
    "fields": []
  },
  {
    "name": "OFFERS_ALCOHOL",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "POPULARITY_SCORE",
    "mode": "NULLABLE",
    "type": "INTEGER",
    "description": "",
    "fields": []
  },
  {
    "name": "URL_FACEBOOK",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "FACEBOOK_REVIEW_RATING",
    "mode": "NULLABLE",
    "type": "FLOAT",
    "description": "",
    "fields": []
  },
  {
    "name": "FACEBOOK_REVIEW_COUNT",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "URL_YELP",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "YELP_REVIEW_RATING",
    "mode": "NULLABLE",
    "type": "FLOAT",
    "description": "",
    "fields": []
  },
  {
    "name": "YELP_REVIEW_COUNT",
    "mode": "NULLABLE",
    "type": "INTEGER",
    "description": "",
    "fields": []
  },
  {
    "name": "URL_GOOGLE",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "GOOGLE_REVIEW_RATING",
    "mode": "NULLABLE",
    "type": "FLOAT",
    "description": "",
    "fields": []
  },
  {
    "name": "GOOGLE_REVIEW_COUNT",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "URL_FOURSQUARE",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "URL_INSTAGRAM",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "URL_TRIPADVISOR",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "TRIP_ADVISOR_REVIEW_RATING",
    "mode": "NULLABLE",
    "type": "FLOAT",
    "description": "",
    "fields": []
  },
  {
    "name": "TRIP_ADVISOR_REVIEW_COUNT",
    "mode": "NULLABLE",
    "type": "INTEGER",
    "description": "",
    "fields": []
  },
  {
    "name": "URL_MICHELIN",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "DELIVERY_LINKS",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "RESERVATION_LINKS",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "URL_BOOKING_DOT_COM",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "URL_HOTELS_DOT_COM",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "PURCHASE_POTENTIAL_SUM_TOTAL",
    "mode": "NULLABLE",
    "type": "INTEGER",
    "description": "",
    "fields": []
  },
  {
    "name": "PURCHASE_POTENTIAL_SUM_FOOD",
    "mode": "NULLABLE",
    "type": "INTEGER",
    "description": "",
    "fields": []
  },
  {
    "name": "PURCHASE_POTENTIAL_SUM_BEVERAGE",
    "mode": "NULLABLE",
    "type": "INTEGER",
    "description": "",
    "fields": []
  },
  {
    "name": "PURCHASE_POTENTIAL_SUM_FOOD_AND_BEVERAGE",
    "mode": "NULLABLE",
    "type": "INTEGER",
    "description": "",
    "fields": []
  },
  {
    "name": "PURCHASE_POTENTIAL_DISPOSABLE",
    "mode": "NULLABLE",
    "type": "INTEGER",
    "description": "",
    "fields": []
  },
  {
    "name": "CHD_ID",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "CONTACT_FIRST_NAME",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "CONTACT_LAST_NAME",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "CONTACT_TITLE",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": "",
    "fields": []
  }
]


def check_file() -> bool:
    """ShortCircuit callable: clean CHD CSVs in landing zone or skip branch.

    1. List `CHD_FILE_PREFIX_*.csv` blobs outside `processed/`.
    2. Strip whitespace from numeric/integer columns.
    3. Coerce invalid integer strings (e.g. text in PHONE) to empty → NULL.
    4. Overwrite blob in place; return False when nothing to load.
    """
    storage_client = storage.Client()
    bucket = storage_client.bucket(CHD_LANDING_BUCKET)

    numeric_fields = [
        field["name"]
        for field in schema_fields
        if field["type"] in {"INTEGER", "FLOAT", "NUMERIC", "INT64", "FLOAT64"}
    ]
    integer_fields = [
        field["name"]
        for field in schema_fields
        if field["type"] in {"INTEGER", "INT64"}
    ]

    logger.info(
        "CHD clean: %s numeric fields, %s integer fields",
        len(numeric_fields),
        len(integer_fields),
    )

    blobs_to_process = []
    total_size = 0
    for blob in bucket.list_blobs():
        name = str(blob.name)
        if (
            "processed/" not in name
            and CHD_FILE_PREFIX in name
            and name.endswith(".csv")
        ):
            blobs_to_process.append(blob)
            total_size += blob.size or 0

    logger.info("Found %s CHD files (%s bytes)", len(blobs_to_process), total_size)
    if total_size == 0:
        return False

    for blob in blobs_to_process:
        try:
            logger.info("Cleaning %s", blob.name)
            file_content = blob.download_as_text(encoding="utf-8")
            csv_reader = csv.DictReader(io.StringIO(file_content))
            fieldnames = csv_reader.fieldnames
            if not fieldnames:
                logger.warning("No header in %s — skip", blob.name)
                continue

            cleaned_rows = []
            rows_cleaned_count = 0
            string_to_null_count = 0

            for row_num, row in enumerate(csv_reader, start=2):
                cleaned_row = {}
                row_was_cleaned = False
                for field_name, value in row.items():
                    if field_name in numeric_fields:
                        if value is not None and str(value).strip():
                            original_value = str(value)
                            cleaned_value = original_value.strip()
                            cleaned_value = re.sub(r"\s+", "", cleaned_value)

                            if field_name in integer_fields:
                                if not is_valid_integer(cleaned_value):
                                    string_to_null_count += 1
                                    cleaned_value = ""
                                    row_was_cleaned = True
                                else:
                                    try:
                                        cleaned_value = str(int(cleaned_value))
                                    except (ValueError, OverflowError):
                                        cleaned_value = ""

                            if not cleaned_value:
                                cleaned_value = ""
                            if (
                                original_value != cleaned_value
                                and field_name not in integer_fields
                            ):
                                row_was_cleaned = True
                            cleaned_row[field_name] = cleaned_value
                        else:
                            cleaned_row[field_name] = value if value else ""
                    else:
                        cleaned_row[field_name] = value

                if row_was_cleaned:
                    rows_cleaned_count += 1
                cleaned_rows.append(cleaned_row)

            logger.info(
                "  cleaned rows=%s / %s; invalid-int→NULL=%s",
                rows_cleaned_count,
                len(cleaned_rows),
                string_to_null_count,
            )

            output = io.StringIO()
            csv_writer = csv.DictWriter(output, fieldnames=fieldnames)
            csv_writer.writeheader()
            csv_writer.writerows(cleaned_rows)
            blob.upload_from_string(output.getvalue(), content_type="text/csv")
        except Exception:
            logger.exception("Failed cleaning %s — continue other files", blob.name)
            continue

    return True


