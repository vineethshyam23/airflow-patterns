"""
Avro bulk ingest for payment-product KYC status.

dbt (triggered by the DAG) refreshes refined.payment_kyc_export.
This module: BQ SELECT → Avro encode → chunked POST to
/ingestbulk/{country}/{schema_id}.

Credentials and schema ids come from Airflow Variables only.

Source (read-only):
  dags/horeca_digital/dana_dishpay_kyc_export.py

Sanitized fixes vs production module:
  - Avro schema parsed once per send (source parsed every row)
  - HTTP errors raise instead of only printing the body
  - 401 retry passes the original payload (source dropped it)
  - Schema id / ingest base externalized to Variables
  - GCP project / dataset / company names generalized
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

from kyc_query import PaymentKyc

ENV_VAR_NAME = "env"
ENV = os.environ.get(ENV_VAR_NAME, Variable.get(ENV_VAR_NAME))

if ENV == "DEV":
    OAUTH_USERNAME = Variable.get("event_api_username")
    OAUTH_PASSWORD = Variable.get("event_api_password")
    CLIENT_ID = Variable.get("event_api_client_id_dev")
    CLIENT_SECRET = Variable.get("event_api_client_secret_dev")
    OAUTH2_URL = Variable.get("event_api_oauth2_url_dev")
    BIGQUERY_PROJECT = "dwh_project_dev"
    KYC_SCHEMA_ID = Variable.get(
        "payment_kyc_schema_id_dev", default_var="PAYMENT_KYC_SCHEMA_DEV"
    )
    SCHEMA_NAME = "payment_kyc_dev"
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
    KYC_SCHEMA_ID = Variable.get(
        "payment_kyc_schema_id", default_var="PAYMENT_KYC_SCHEMA_PROD"
    )
    SCHEMA_NAME = "payment_kyc"
    INGEST_BASE = Variable.get(
        "event_api_ingest_base",
        default_var="https://api.example.com/event-ingest/bulk",
    )


KYC_AVRO_SCHEMA = json.dumps(
    {
        "namespace": "company",
        "type": "record",
        "name": SCHEMA_NAME,
        "doc": "Payment-product KYC onboarding status for partner markets",
        "gdpr_info": {"table_PII": "no", "column_PII": []},
        "fields": [
            {
                "name": "establishment_id",
                "type": ["null", "string"],
                "default": None,
                "doc": "Unique establishment identifier",
            },
            {
                "name": "country_code",
                "type": ["null", "string"],
                "default": None,
                "doc": "ISO 2-letter country code",
            },
            {
                "name": "kyc_created_dt",
                "type": ["null", "string"],
                "default": None,
                "doc": "KYC created timestamp (YYYY-MM-DD HH:MM:SS string)",
            },
            {
                "name": "kyc_modified_dt",
                "type": ["null", "string"],
                "default": None,
                "doc": "KYC last modified date (YYYY-MM-DD string)",
            },
            {
                "name": "kyc_step",
                "type": ["null", "long"],
                "default": None,
                "doc": "Current onboarding step number",
            },
            {
                "name": "kyc_step_details",
                "type": ["null", "string"],
                "default": None,
                "doc": "Details for the current KYC step",
            },
            {
                "name": "kyc_status",
                "type": ["null", "string"],
                "default": None,
                "doc": "KYC verification status",
            },
            {
                "name": "kyc_duration_day",
                "type": ["null", "long"],
                "default": None,
                "doc": "KYC process duration in days",
            },
            {
                "name": "kyc_duration_month",
                "type": ["null", "long"],
                "default": None,
                "doc": "KYC process duration in months",
            },
            {
                "name": "kyc_onboarding_successful",
                "type": ["null", "long"],
                "default": None,
                "doc": "1 if onboarding completed successfully",
            },
            {
                "name": "kyc_first_attempt",
                "type": ["null", "long"],
                "default": None,
                "doc": "1 if this is the first KYC attempt",
            },
            {
                "name": "kyc_adyen_pending_validation",
                "type": ["null", "long"],
                "default": None,
                "doc": "Count of submissions pending PSP validation",
            },
            {
                "name": "kyc_adyen_error_validation",
                "type": ["null", "long"],
                "default": None,
                "doc": "Count of submissions with PSP validation errors",
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
            # Retry with the same payload — production dropped `data` here.
            return self.post_json(url, data=data)

        response.raise_for_status()
        return response.json()


def _encode_bytes(payload: dict, schema) -> str:
    writer = avro.io.DatumWriter(schema)
    bytes_writer = io.BytesIO()
    encoder = avro.io.BinaryEncoder(bytes_writer)
    writer.write(payload, encoder)
    return base64.b64encode(bytes_writer.getvalue()).decode("utf-8")


def send_payment_kyc_data(country: str = "pl") -> None:
    """Query refined KYC rows and POST Avro chunks to the event bus."""
    client = EventApiClient(OAUTH2_URL, CLIENT_ID, CLIENT_SECRET)
    # Parse once. Production re-parsed KYC_SCHEMA inside the row loop.
    schema = avro.schema.parse(KYC_AVRO_SCHEMA)
    query = PaymentKyc.get_send_query()

    logging.info("%s - Executing payment_kyc query...", datetime.now())
    bq = bigquery.Client(project=BIGQUERY_PROJECT)
    results = bq.query(query).result()

    encoded: List[str] = []
    for row in results:
        row_dict = dict(row.items())
        encoded.append(_encode_bytes(row_dict, schema))

    logging.info(
        "%s - %s rows encoded for %s",
        datetime.now(),
        len(encoded),
        country.upper(),
    )

    base_url = f"{INGEST_BASE.rstrip('/')}/{country.lower()}/{KYC_SCHEMA_ID}"
    sent = 0
    for chunk in chunks(encoded, 500):
        sent += len(chunk)
        body = {"records": [{"value": record} for record in chunk]}
        logging.info(
            "%s - payment_kyc %s chunk progress %s / %s",
            datetime.now(),
            country.upper(),
            sent,
            len(encoded),
        )
        client.post_json(url=base_url, data=json.dumps(body))

    logging.info(
        "%s - Process completed for payment_kyc (%s)",
        datetime.now(),
        country.upper(),
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    # Schema parse smoke check only — no network / BQ calls.
    avro.schema.parse(KYC_AVRO_SCHEMA)
    print("Avro schema OK:", SCHEMA_NAME)
