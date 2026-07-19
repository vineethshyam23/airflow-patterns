"""
Avro bulk ingest of Salesforce asset history deltas to an external event API.

Reads a BigQuery delta query, encodes each row as Avro binary (base64),
chunks to 500 records, and POSTs to /ingestbulk/{country}/{schema_id}.

Credentials come from Airflow Variables only — nothing hard-coded.

Source (read-only): dags/horeca_digital/dana_sfdc_asset_export.py
"""

from __future__ import annotations

import base64
import io
import json
import logging
import os
from datetime import date, datetime
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
    SCHEMA_ID = Variable.get("sfdc_asset_event_schema_id_dev", default_var="SCHEMA_ID_DEV")
    SCHEMA_NAME = "sfdc_asset_history_dev"
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
    SCHEMA_ID = Variable.get("sfdc_asset_event_schema_id", default_var="SCHEMA_ID_PROD")
    SCHEMA_NAME = "sfdc_asset_history"
    INGEST_BASE = Variable.get(
        "event_api_ingest_base",
        default_var="https://api.example.com/event-ingest/bulk",
    )

# Avro contract for the event bus. Field set mirrors the BQ send query.
# Dates use Avro logicalType date (days since epoch) where the bus expects it.
SFDC_ASSET_AVRO_SCHEMA = json.dumps(
    {
        "namespace": "company",
        "type": "record",
        "name": SCHEMA_NAME,
        "doc": "Salesforce CRM asset / install history for event ingest",
        "fields": [
            {"name": "establishment_uid", "type": ["null", "string"], "doc": "Establishment UID"},
            {"name": "product", "type": ["null", "string"], "doc": "Product name"},
            {"name": "channel_v2", "type": ["null", "string"], "doc": "Sales channel"},
            {"name": "referrer", "type": ["null", "string"], "doc": "Referrer"},
            {"name": "activated_by", "type": ["null", "string"], "doc": "Activated by"},
            {
                "name": "install_date",
                "type": ["null", {"type": "int", "logicalType": "date"}],
                "default": None,
                "doc": "Install date",
            },
            {
                "name": "disabled_date",
                "type": ["null", {"type": "int", "logicalType": "date"}],
                "default": None,
                "doc": "Disabled date",
            },
            {"name": "status", "type": ["null", "string"], "doc": "Asset status"},
            {"name": "establishment_name", "type": ["null", "string"], "doc": "Establishment name"},
            {"name": "shipping_postal_code", "type": ["null", "string"]},
            {"name": "shipping_city", "type": ["null", "string"]},
            {"name": "shipping_street", "type": ["null", "string"]},
            {"name": "person_email", "type": ["null", "string"], "doc": "Contact email"},
            {"name": "email_permission", "type": ["null", "boolean"]},
            {"name": "account_id_long", "type": ["null", "string"], "doc": "CRM account id"},
            {"name": "crm_metro_id", "type": ["null", "string"], "doc": "CRM metro id custom field"},
            {"name": "store_id", "type": ["null", "int"]},
            {"name": "crm_account_identifier", "type": ["null", "long"]},
            {"name": "vat_id", "type": ["null", "string"]},
            {"name": "metro_id", "type": ["null", "long"]},
            {"name": "cust_no", "type": ["null", "int"], "default": None},
            {"name": "home_store_id", "type": ["null", "int"], "default": None},
            {"name": "create_ts", "type": ["null", "string"], "default": None},
            {"name": "update_ts", "type": ["null", "string"], "default": None},
            {
                "name": "_ldts",
                "type": {"type": "int", "logicalType": "date"},
                "doc": "Load date",
            },
        ],
    }
)


def chunks(items: List, size: int) -> Iterator[List]:
    for i in range(0, len(items), size):
        yield items[i : i + size]


def _b64(s: str) -> str:
    return base64.b64encode(s.encode()).decode()


def _avro_date(value):
    """BigQuery DATE → Avro logical date (int days since 1970-01-01) or None."""
    if value is None:
        return None
    # google.cloud.bigquery returns datetime.date for DATE columns
    if isinstance(value, date):
        return (value - date(1970, 1, 1)).days
    return value


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
        response = requests.post(self.token_url, headers=headers, params=params, timeout=60)
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

        # Token expiry mid-run — refresh once and retry the same chunk.
        if response.status_code == 401:
            self.token = None
            return self.post_json(url, data=data)

        response.raise_for_status()
        return response.json()


def _encode_row(row, schema) -> str:
    writer = avro.io.DatumWriter(schema)
    bytes_writer = io.BytesIO()
    encoder = avro.io.BinaryEncoder(bytes_writer)
    payload = {
        "establishment_uid": row["establishment_UID"],
        "product": row["Product"],
        "channel_v2": row["ChannelV2"],
        "referrer": row["Referrer"],
        "activated_by": row["ActivatedBy"],
        "install_date": _avro_date(row["InstallDate"]),
        "disabled_date": _avro_date(row["DisabledDate"]),
        "status": row["Status"],
        "establishment_name": row["Establishment_name"],
        "shipping_postal_code": row["ShippingPostalCode"],
        "shipping_city": row["ShippingCity"],
        "shipping_street": row["ShippingStreet"],
        "person_email": row["PersonEmail"],
        "email_permission": row["email_permission"],
        "account_id_long": row["AccountId_Long"],
        "crm_metro_id": row["Crm_Metro_Id"],
        "store_id": row["Store_Id"],
        "crm_account_identifier": row["Crm_Account_Identifier"],
        "vat_id": row["VAT_id"],
        "metro_id": row["metro_id"],
        "cust_no": row["cust_no"],
        "home_store_id": row["home_store_id"],
        "create_ts": row["_create_ts"],
        "update_ts": row["_update_ts"],
        "_ldts": _avro_date(row["_ldts"]),
    }
    writer.write(payload, encoder)
    return base64.b64encode(bytes_writer.getvalue()).decode("utf-8")


def send_sfdc_asset_history_data(query: str, country: str = "es") -> None:
    """
    Run the delta query, Avro-encode rows, POST in chunks of 500.

    Empty result is fine — quiet days after a large install wave.
    """
    client = EventApiClient(OAUTH2_URL, CLIENT_ID, CLIENT_SECRET)
    schema = avro.schema.parse(SFDC_ASSET_AVRO_SCHEMA)

    logging.info("%s - Executing SFDC asset history query...", datetime.now())
    bq = bigquery.Client(project=BIGQUERY_PROJECT)
    results = bq.query(query).result()

    encoded: List[str] = []
    logging.info(
        "%s - Processing SFDC asset history result set - %s...",
        datetime.now(),
        country,
    )
    for row in results:
        encoded.append(_encode_row(row, schema))

    logging.info(
        "%s - %s rows to send for %s", datetime.now(), len(encoded), country.upper()
    )

    base_url = f"{INGEST_BASE.rstrip('/')}/{country}/{SCHEMA_ID}"
    sent = 0
    for chunk in chunks(encoded, 500):
        sent += len(chunk)
        payload = {"records": [{"value": record} for record in chunk]}
        logging.info(
            "%s - %s chunk progress %s / %s",
            datetime.now(),
            country.upper(),
            sent,
            len(encoded),
        )
        client.post_json(url=base_url, data=json.dumps(payload))

    logging.info(
        "%s - SFDC asset history ingest completed for %s",
        datetime.now(),
        country.upper(),
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    raise SystemExit(
        "Call send_sfdc_asset_history_data(query, country) from Airflow or a notebook"
    )
