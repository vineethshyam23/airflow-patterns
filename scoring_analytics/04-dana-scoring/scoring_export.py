"""
Avro bulk ingest of scoring deltas to an external event API.

Reads a BigQuery delta query, encodes each row as Avro binary (base64),
chunks to 500 records, and POSTs to /ingestbulk/{country}/{schema_id}.

Credentials come from Airflow Variables only — nothing hard-coded.

Source (read-only): dags/horeca_digital/dana_scoring_export.py
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
    SCHEMA_ID = Variable.get("scoring_event_schema_id_dev", default_var="SCHEMA_ID_DEV")
    SCHEMA_NAME = "scoring_potential_customers_dev"
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
    SCHEMA_ID = Variable.get("scoring_event_schema_id", default_var="SCHEMA_ID_PROD")
    SCHEMA_NAME = "scoring_potential_customers"
    INGEST_BASE = Variable.get(
        "event_api_ingest_base",
        default_var="https://api.example.com/event-ingest/bulk",
    )

# Avro contract for the event bus. Field set mirrors the BQ send query.
SCORING_AVRO_SCHEMA = json.dumps(
    {
        "namespace": "company",
        "type": "record",
        "name": SCHEMA_NAME,
        "doc": "Scoring payload for potential customers (FBO/NBO)",
        "fields": [
            {"name": "metro_id", "type": "long", "doc": "Customer child id"},
            {"name": "establishment_id", "type": "string"},
            {"name": "establishment_name", "type": "string"},
            {"name": "street_name", "type": "string"},
            {"name": "street_number", "type": "string"},
            {"name": "postal_code", "type": "string"},
            {"name": "city", "type": "string"},
            {"name": "iso_code", "type": "string"},
            {"name": "manager_information", "type": "string"},
            {"name": "first_name", "type": "string"},
            {"name": "mobilephone", "type": "string"},
            {"name": "establishment_type", "type": "string"},
            {"name": "potential_level", "type": "int"},
            {"name": "bundle_recommendation", "type": "string"},
            {"name": "_ldts", "type": "string"},
            {
                "name": "bundle_potential_score",
                "type": ["null", "float"],
                "default": None,
            },
            {
                "name": "pos_recommendation",
                "type": ["null", "int"],
                "default": None,
            },
            {
                "name": "pos_potential_score",
                "type": ["null", "float"],
                "default": None,
            },
            {
                "name": "pay_recommendation",
                "type": ["null", "int"],
                "default": None,
            },
            {
                "name": "pay_potential_score",
                "type": ["null", "float"],
                "default": None,
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

        # Token expiry mid-run is common on long country loops — refresh once.
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
        "metro_id": row["metro_id"],
        "establishment_id": row["establishment_id"],
        "establishment_name": row["establishment_name"],
        "street_name": row["street_name"],
        "street_number": row["street_number"],
        "postal_code": row["postal_code"],
        "city": row["city"],
        "iso_code": row["iso_code"],
        "manager_information": row["manager_information"],
        "first_name": row["first_name"],
        "mobilephone": row["mobilephone"],
        "establishment_type": row["establishment_type"],
        "potential_level": int(row["potential_level"]),
        "bundle_recommendation": row["bundle_recommendation"],
        "_ldts": row["_ldts"],
        "bundle_potential_score": (
            float(row["bundle_potential_score"])
            if row["bundle_potential_score"] is not None
            else None
        ),
        "pos_recommendation": (
            int(row["pos_recommendation"])
            if row["pos_recommendation"] is not None
            else None
        ),
        "pos_potential_score": (
            float(row["pos_potential_score"])
            if row["pos_potential_score"] is not None
            else None
        ),
        "pay_recommendation": (
            int(row["pay_recommendation"])
            if row["pay_recommendation"] is not None
            else None
        ),
        "pay_potential_score": (
            float(row["pay_potential_score"])
            if row["pay_potential_score"] is not None
            else None
        ),
    }
    writer.write(payload, encoder)
    return base64.b64encode(bytes_writer.getvalue()).decode("utf-8")


def send_scoring_data(country: str, query: str) -> None:
    """
    Run the delta query, Avro-encode rows, POST in chunks of 500.

    Empty result sets are fine (markets without a model, quiet months).
    """
    client = EventApiClient(OAUTH2_URL, CLIENT_ID, CLIENT_SECRET)
    schema = avro.schema.parse(SCORING_AVRO_SCHEMA)

    logging.info("%s - Executing scoring delta query for %s", datetime.now(), country)
    bq = bigquery.Client(project=BIGQUERY_PROJECT)
    results = bq.query(query).result()

    encoded: List[str] = []
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

    logging.info("%s - Scoring ingest completed for %s", datetime.now(), country.upper())


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    # Manual smoke: provide country + SQL via env / notebook, not hard-coded here.
    raise SystemExit("Call send_scoring_data(country, query) from Airflow or a notebook")
