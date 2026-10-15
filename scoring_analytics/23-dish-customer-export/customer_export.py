"""
Avro bulk ingest for multi-country platform-customer footprint.

DAG stage order (see dag_dish_customer_export.py):
  1. Parallel BQ inserts → shared staging table (truncate first country)
  2. dbt Cloud job refreshes refined.platform_customer_export
  3. Per-country SELECT → Avro → chunked POST

Credentials and schema ids come from Airflow Variables only.

Source (read-only):
  dags/horeca_digital/dana_DISH_customer_export.py

Sanitized fixes vs production module:
  - Avro schema parsed once per send (source parsed every row)
  - query.result() called once (source called twice)
  - HTTP errors raise instead of only printing the body
  - 401 retry keeps the original payload
  - Schema id / ingest base externalized to Variables
  - Commented credentials removed
  - GCP project / company / product brand names generalized
"""

from __future__ import annotations

import base64
import io
import json
import logging
import os
from datetime import datetime
from typing import Iterator, List

import avro.io
import avro.schema
import requests
from airflow.models import Variable
from google.cloud import bigquery

ENV_VAR_NAME = "env"
ENV = os.environ.get(ENV_VAR_NAME, Variable.get(ENV_VAR_NAME))

if ENV == "DEV":
    OAUTH_USERNAME = Variable.get("event_api_username")
    OAUTH_PASSWORD = Variable.get("event_api_password")
    CLIENT_ID = Variable.get("event_api_client_id_dev")
    CLIENT_SECRET = Variable.get("event_api_client_secret_dev")
    OAUTH2_URL = Variable.get("event_api_oauth2_url_dev")
    BIGQUERY_PROJECT = "dwh_project_dev"
    CUSTOMER_SCHEMA_ID = Variable.get(
        "platform_customer_schema_id_dev",
        default_var="PLATFORM_CUSTOMER_SCHEMA_DEV",
    )
    SCHEMA_NAME = "platform_customers_dev"
    INGEST_BASE = Variable.get(
        "event_api_ingest_base_dev",
        default_var="https://api.example.com/event-ingest/bulk",
    )
else:
    OAUTH_USERNAME = Variable.get("event_api_username")
    OAUTH_PASSWORD = Variable.get("event_api_password")
    CLIENT_ID = Variable.get("event_api_client_id")
    CLIENT_SECRET = Variable.get("event_api_client_secret")
    OAUTH2_URL = Variable.get("event_api_oauth2_url")
    BIGQUERY_PROJECT = "dwh_project"
    CUSTOMER_SCHEMA_ID = Variable.get(
        "platform_customer_schema_id",
        default_var="PLATFORM_CUSTOMER_SCHEMA_PROD",
    )
    SCHEMA_NAME = "platform_customers"
    INGEST_BASE = Variable.get(
        "event_api_ingest_base",
        default_var="https://api.example.com/event-ingest/bulk",
    )


CUSTOMER_AVRO_SCHEMA = json.dumps(
    {
        "namespace": "company",
        "type": "record",
        "name": SCHEMA_NAME,
        "doc": "Wholesale IDs that are active platform product customers",
        "gdpr_info": {"table_PII": "no", "column_PII": []},
        "fields": [
            {"name": "wholesale_id", "type": "long", "doc": "Wholesale child ID"},
            {"name": "country_iso", "type": "string", "doc": "ISO code"},
            {"name": "cust_no", "type": "int", "doc": "Customer number"},
            {"name": "home_store_id", "type": "int", "doc": "Home store ID"},
            {
                "name": "platform_active_customer",
                "type": "string",
                "doc": "Is platform active customer",
            },
            {
                "name": "platform_bundle",
                "type": "string",
                "doc": "Bundle: Starter / Pro. Reservation / Pro. Order / Premium",
            },
            {
                "name": "platform_bundle_timestamp",
                "type": "string",
                "doc": "Bundle timestamp",
            },
            {"name": "has_POS", "type": "string", "doc": "Has POS - Y or N"},
            {"name": "POS_timestamp", "type": "string", "doc": "POS timestamp"},
            {
                "name": "has_Reservation",
                "type": "string",
                "doc": "Has Reservation - Y or N",
            },
            {"name": "has_Website", "type": "string", "doc": "Has Website - Y or N"},
            {
                "name": "has_Weblisting",
                "type": "string",
                "doc": "Has Weblisting - Y or N",
            },
            {"name": "has_Order", "type": "string", "doc": "Has Order - Y or N"},
            {"name": "has_Menukit", "type": "string", "doc": "Has Menukit - Y or N"},
            {
                "name": "Website_creation_ts",
                "type": ["null", "string"],
                "default": None,
                "doc": "Website creation timestamp",
            },
            {
                "name": "Reservation_creation_ts",
                "type": ["null", "string"],
                "default": None,
                "doc": "Reservation creation timestamp",
            },
            {
                "name": "WebListing_creation_ts",
                "type": ["null", "string"],
                "default": None,
                "doc": "Web listing creation timestamp",
            },
            {
                "name": "Order_creation_ts",
                "type": ["null", "string"],
                "default": None,
                "doc": "Order creation timestamp",
            },
            {
                "name": "Website_deletion_ts",
                "type": ["null", "string"],
                "default": None,
                "doc": "Website deletion timestamp",
            },
            {
                "name": "Reservation_deletion_ts",
                "type": ["null", "string"],
                "default": None,
                "doc": "Reservation deletion timestamp",
            },
            {
                "name": "WebListing_deletion_ts",
                "type": ["null", "string"],
                "default": None,
                "doc": "Web listing deletion timestamp",
            },
            {
                "name": "Order_deletion_ts",
                "type": ["null", "string"],
                "default": None,
                "doc": "Order deletion timestamp",
            },
            {
                "name": "POS_deletion_ts",
                "type": ["null", "string"],
                "default": None,
                "doc": "POS deletion timestamp",
            },
            {
                "name": "Starter_creation_ts",
                "type": ["null", "string"],
                "default": None,
                "doc": "Starter bundle creation",
            },
            {
                "name": "Starter_deletion_ts",
                "type": ["null", "string"],
                "default": None,
                "doc": "Starter bundle deletion",
            },
            {
                "name": "ProfReservation_creation_ts",
                "type": ["null", "string"],
                "default": None,
                "doc": "Pro Reservation bundle creation",
            },
            {
                "name": "ProfReservation_deletion_ts",
                "type": ["null", "string"],
                "default": None,
                "doc": "Pro Reservation bundle deletion",
            },
            {
                "name": "ProfOrder_creation_ts",
                "type": ["null", "string"],
                "default": None,
                "doc": "Pro Order bundle creation",
            },
            {
                "name": "ProfOrder_deletion_ts",
                "type": ["null", "string"],
                "default": None,
                "doc": "Pro Order bundle deletion",
            },
            {
                "name": "Premium_creation_ts",
                "type": ["null", "string"],
                "default": None,
                "doc": "Premium bundle creation",
            },
            {
                "name": "Premium_deletion_ts",
                "type": ["null", "string"],
                "default": None,
                "doc": "Premium bundle deletion",
            },
            {
                "name": "POS_Referrer",
                "type": ["null", "string"],
                "default": None,
                "doc": "Referrer of active POS",
            },
            {
                "name": "Bundle_Referrer",
                "type": ["null", "string"],
                "default": None,
                "doc": "Referrer of active bundle",
            },
            {
                "name": "wholesale_account_id",
                "type": ["null", "string"],
                "default": None,
                "doc": "Wholesale account identifier",
            },
            {
                "name": "has_POS_flag",
                "type": ["null", "string"],
                "default": None,
                "doc": "POS indicator flag",
            },
            {
                "name": "date_acquisition",
                "type": ["null", "string"],
                "default": None,
                "doc": "Acquisition date",
            },
            {
                "name": "date_deletion",
                "type": ["null", "string"],
                "default": None,
                "doc": "Deletion date",
            },
            {
                "name": "POS_creation_ts",
                "type": ["null", "string"],
                "default": None,
                "doc": "POS creation timestamp",
            },
            {
                "name": "has_Pay",
                "type": ["null", "string"],
                "default": None,
                "doc": "Pay service indicator",
            },
            {
                "name": "Pay_creation_ts",
                "type": ["null", "string"],
                "default": None,
                "doc": "Pay creation timestamp",
            },
            {
                "name": "Pay_deletion_ts",
                "type": ["null", "string"],
                "default": None,
                "doc": "Pay deletion timestamp",
            },
        ],
    }
)


def chunks(items: List, size: int) -> Iterator[List]:
    for i in range(0, len(items), size):
        yield items[i : i + size]


def _b64(s: str) -> str:
    return base64.b64encode(s.encode()).decode()


class EventApiClient:
    """Password-grant OAuth client with one-shot 401 retry on POST."""

    def __init__(self, token_url: str, client_id: str, client_secret: str):
        self.token_url = token_url
        self.client_id = client_id
        self.client_secret = client_secret
        self.token = None

    def get_token(self) -> str:
        headers = {
            "Authorization": "Basic " + _b64(self.client_id + ":" + self.client_secret),
            "Content-Type": "application/x-www-form-urlencoded",
        }
        params = {
            "grant_type": "password",
            "username": OAUTH_USERNAME,
            "password": OAUTH_PASSWORD,
        }
        response = requests.post(
            self.token_url, headers=headers, params=params, timeout=60
        )
        response.raise_for_status()
        self.token = response.json()["access_token"]
        return self.token

    def post_json(self, url: str, data=None):
        if self.token is None:
            self.get_token()

        headers = {
            "Authorization": "Bearer " + self.token,
            "Content-Type": "application/vnd.company.events.json",
        }
        response = requests.post(url, headers=headers, data=data, timeout=120)

        if response.status_code == 401:
            self.token = None
            return self.post_json(url, data=data)

        response.raise_for_status()
        return response.json()


def _encode_bytes(payload: dict, schema) -> str:
    writer = avro.io.DatumWriter(schema)
    bytes_writer = io.BytesIO()
    encoder = avro.io.BinaryEncoder(bytes_writer)
    writer.write(payload, encoder)
    return base64.b64encode(bytes_writer.getvalue()).decode("utf-8")


def send_platform_customer_data(country: str, query: str) -> None:
    """Query refined customer rows and POST Avro chunks to the event bus."""
    client = EventApiClient(OAUTH2_URL, CLIENT_ID, CLIENT_SECRET)
    # Parse once. Production re-parsed the schema inside the row loop.
    schema = avro.schema.parse(CUSTOMER_AVRO_SCHEMA)

    logging.info("%s - Executing platform_customer query...", datetime.now())
    bq = bigquery.Client(project=BIGQUERY_PROJECT)
    results = bq.query(query).result()

    encoded: List[str] = []
    for row in results:
        encoded.append(
            _encode_bytes(
                {
                    "wholesale_id": row["wholesale_id"],
                    "cust_no": row["cust_no"],
                    "country_iso": row["country_iso"],
                    "home_store_id": row["home_store_id"],
                    "platform_active_customer": row["platform_active_customer"],
                    "platform_bundle": row["platform_bundle"],
                    "platform_bundle_timestamp": row["platform_bundle_timestamp"],
                    "has_POS": row["has_POS"],
                    "POS_timestamp": row["POS_timestamp"],
                    "has_Reservation": row["has_Reservation"],
                    "has_Website": row["has_Website"],
                    "has_Weblisting": row["has_Weblisting"],
                    "has_Order": row["has_Order"],
                    "has_Menukit": row["has_Menukit"],
                    "Website_creation_ts": row["Website_creation_ts"],
                    "Reservation_creation_ts": row["Reservation_creation_ts"],
                    "WebListing_creation_ts": row["WebListing_creation_ts"],
                    "Order_creation_ts": row["Order_creation_ts"],
                    "Website_deletion_ts": row["Website_deletion_ts"],
                    "Reservation_deletion_ts": row["Reservation_deletion_ts"],
                    "WebListing_deletion_ts": row["WebListing_deletion_ts"],
                    "Order_deletion_ts": row["Order_deletion_ts"],
                    "POS_deletion_ts": row["POS_deletion_ts"],
                    "Starter_creation_ts": row["Starter_creation_ts"],
                    "Starter_deletion_ts": row["Starter_deletion_ts"],
                    "ProfReservation_creation_ts": row["ProfReservation_creation_ts"],
                    "ProfReservation_deletion_ts": row["ProfReservation_deletion_ts"],
                    "ProfOrder_creation_ts": row["ProfOrder_creation_ts"],
                    "ProfOrder_deletion_ts": row["ProfOrder_deletion_ts"],
                    "Premium_creation_ts": row["Premium_creation_ts"],
                    "Premium_deletion_ts": row["Premium_deletion_ts"],
                    "POS_Referrer": row["POS_Referrer"],
                    "Bundle_Referrer": row["Bundle_Referrer"],
                    "wholesale_account_id": row["wholesale_account_id"],
                    "has_POS_flag": row["has_POS_flag"],
                    "date_acquisition": row["date_acquisition"],
                    "date_deletion": row["date_deletion"],
                    "POS_creation_ts": row["POS_creation_ts"],
                    "has_Pay": row["has_Pay"],
                    "Pay_creation_ts": row["Pay_creation_ts"],
                    "Pay_deletion_ts": row["Pay_deletion_ts"],
                },
                schema,
            )
        )

    logging.info(
        "%s - %s rows encoded for %s",
        datetime.now(),
        len(encoded),
        country.upper(),
    )

    base_url = f"{INGEST_BASE.rstrip('/')}/{country.lower()}/{CUSTOMER_SCHEMA_ID}"
    sent = 0
    for chunk in chunks(encoded, 500):
        sent += len(chunk)
        body = {"records": [{"value": record} for record in chunk]}
        logging.info(
            "%s - platform_customer %s chunk progress %s / %s",
            datetime.now(),
            country.upper(),
            sent,
            len(encoded),
        )
        client.post_json(url=base_url, data=json.dumps(body))

    logging.info(
        "%s - Process completed for platform_customer (%s)",
        datetime.now(),
        country.upper(),
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    avro.schema.parse(CUSTOMER_AVRO_SCHEMA)
    print("Avro schema OK:", SCHEMA_NAME)
