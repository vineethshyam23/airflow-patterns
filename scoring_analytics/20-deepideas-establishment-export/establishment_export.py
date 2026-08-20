"""
Avro bulk ingest for establishment-attribute enrichment rows.

Pulls a SELECT (usually the today/yesterday hash-delta from
delta_queries.py), Avro-encodes each row, and POSTs chunks of 500 to
/ingestbulk/{country}/{schema_id}.

Credentials and schema ids come from Airflow Variables only.

Source (read-only):
  dags/horeca_digital/dana_deepideas_establishment_export.py

Sanitized fixes vs production module:
  - Schema id / ingest base externalized to Variables
  - GCP project / Avro namespace / field names generalized
  - wholesale_id replaces metro_id; store_distance_* replaces mcc_distance_*
  - 401 clears token and retries the same payload
  - Schema parsed once per send (not once per row)
"""

from __future__ import annotations

import base64
import io
import json
import logging
import os
from datetime import datetime
from typing import Any, Dict, Iterable, List

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
    SCHEMA_ID = Variable.get(
        "establishment_attrs_schema_id_dev",
        default_var="ESTABLISHMENT_ATTRS_SCHEMA_DEV",
    )
    SCHEMA_NAME = "establishment_attrs_dev"
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
    SCHEMA_ID = Variable.get(
        "establishment_attrs_schema_id",
        default_var="ESTABLISHMENT_ATTRS_SCHEMA_PROD",
    )
    SCHEMA_NAME = "establishment_attrs"
    INGEST_BASE = Variable.get(
        "event_api_ingest_base",
        default_var="https://api.example.com/event-ingest/bulk",
    )

AVRO_SCHEMA = json.dumps(
    {
        "namespace": "company",
        "type": "record",
        "name": SCHEMA_NAME,
        "doc": (
            "Establishment enrichment attributes for active wholesale "
            "buyers: geo densities, digitalisation, cuisine, menu mix"
        ),
        "gdpr_info": {"table_PII": "no", "column_PII": []},
        "fields": [
            {"name": "wholesale_id", "type": "long", "doc": "Wholesale customer id"},
            {"name": "price_range", "type": "string", "doc": "Price category"},
            {
                "name": "popularity_rate",
                "type": "string",
                "doc": "Average of available public ratings",
            },
            {
                "name": "store_distance_air_km",
                "type": "string",
                "doc": "Flying distance to nearest wholesale store (km)",
            },
            {
                "name": "store_distance_km",
                "type": "string",
                "doc": "Driving distance to nearest wholesale store (km)",
            },
            {
                "name": "store_distance_minutes",
                "type": "string",
                "doc": "Driving duration to nearest wholesale store (min)",
            },
            {
                "name": "competitor_density",
                "type": "int",
                "doc": "Establishments within 500m",
            },
            {
                "name": "has_online_reservation",
                "type": "string",
                "doc": "TRUE if online reservation offered",
            },
            {
                "name": "has_delivery_takeaway",
                "type": "string",
                "doc": "TRUE if delivery and/or takeaway offered",
            },
            {"name": "poi_density", "type": "int", "doc": "POIs within 300m"},
            {
                "name": "cuisine_competitor_density",
                "type": "int",
                "doc": "Same-cuisine establishments within 500m",
            },
            {
                "name": "discounter_density",
                "type": "string",
                "doc": "Discounters within 500m",
            },
            {
                "name": "supermarket_density",
                "type": "string",
                "doc": "Supermarkets within 500m",
            },
            {
                "name": "cash_carry_density",
                "type": "string",
                "doc": "Cash-and-carry markets within 5km",
            },
            {
                "name": "establishment_type",
                "type": "string",
                "doc": "Harmonized establishment type",
            },
            {"name": "cuisine_type", "type": "string", "doc": "Harmonized cuisine type"},
            {
                "name": "digitalisation_index",
                "type": "int",
                "doc": "Digitalisation level of the establishment",
            },
            {"name": "zip_community_type", "type": "string", "doc": "Location type"},
            {
                "name": "purchasing_power_person",
                "type": "string",
                "doc": "Purchasing power per capita in postal district",
            },
            {
                "name": "population_density",
                "type": "string",
                "doc": "Population density in postal district",
            },
            {
                "name": "food_proportion",
                "type": "string",
                "doc": "Share of food items on the menu",
            },
            {
                "name": "avg_food_price",
                "type": "string",
                "doc": "Average price of food menu items",
            },
        ],
    }
)

CHUNK_SIZE = 500
log = logging.getLogger(__name__)

FIELD_ORDER = [
    "wholesale_id",
    "price_range",
    "popularity_rate",
    "store_distance_air_km",
    "store_distance_km",
    "store_distance_minutes",
    "competitor_density",
    "has_online_reservation",
    "has_delivery_takeaway",
    "poi_density",
    "cuisine_competitor_density",
    "discounter_density",
    "supermarket_density",
    "cash_carry_density",
    "establishment_type",
    "cuisine_type",
    "digitalisation_index",
    "zip_community_type",
    "purchasing_power_person",
    "population_density",
    "food_proportion",
    "avg_food_price",
]


def _b64(s: str) -> str:
    return base64.b64encode(s.encode()).decode()


def chunks(items: List[Any], n: int) -> Iterable[List[Any]]:
    for i in range(0, len(items), n):
        yield items[i : i + n]


class EventApiClient:
    def __init__(self, token_url: str, client_id: str, client_secret: str):
        self.token_url = token_url
        self.token = None
        self.client_id = client_id
        self.client_secret = client_secret

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
        r = requests.post(self.token_url, headers=headers, params=params, timeout=60)
        r.raise_for_status()
        self.token = r.json()["access_token"]
        return self.token

    def post_events(self, url: str, data: str) -> Dict[str, Any]:
        if self.token is None:
            self.get_token()
        headers = {
            "Authorization": "Bearer " + self.token,
            "Content-Type": "application/vnd.company.events.json",
        }
        r = requests.post(url, headers=headers, data=data, timeout=120)
        if r.status_code == 401:
            # Token expired mid-batch — clear and retry the same payload.
            self.token = None
            return self.post_events(url, data)
        r.raise_for_status()
        return r.json()


def _row_to_avro(row, schema) -> str:
    writer = avro.io.DatumWriter(schema)
    bytes_writer = io.BytesIO()
    encoder = avro.io.BinaryEncoder(bytes_writer)
    payload = {name: row[name] for name in FIELD_ORDER}
    writer.write(payload, encoder)
    return base64.b64encode(bytes_writer.getvalue()).decode("utf-8")


def send_establishment_data(country: str, query: str) -> None:
    """Run BQ query, Avro-encode rows, POST in chunks of CHUNK_SIZE."""
    client_api = EventApiClient(OAUTH2_URL, CLIENT_ID, CLIENT_SECRET)
    schema = avro.schema.parse(AVRO_SCHEMA)
    bq = bigquery.Client(project=BIGQUERY_PROJECT)

    log.info("%s - Executing establishment query...", datetime.now())
    results = bq.query(query).result()

    encoded: List[str] = []
    for row in results:
        encoded.append(_row_to_avro(row, schema))

    log.info("%s - Encoded %s rows; chunking...", datetime.now(), len(encoded))
    base_url = f"{INGEST_BASE.rstrip('/')}/{country}/{SCHEMA_ID}"
    sent = 0
    for chunk in chunks(encoded, CHUNK_SIZE):
        sent += len(chunk)
        payload = {"records": [{"value": row} for row in chunk]}
        log.info(
            "%s - country=%s chunk_progress=%s/%s",
            datetime.now(),
            country.upper(),
            sent,
            len(encoded),
        )
        client_api.post_events(base_url, json.dumps(payload))

    log.info("%s - Establishment export complete (%s rows).", datetime.now(), len(encoded))


if __name__ == "__main__":
    # Smoke: schema parses; no live BQ / HTTP without Composer Variables.
    avro.schema.parse(AVRO_SCHEMA)
    print("schema_ok", SCHEMA_NAME, "fields", len(FIELD_ORDER))
