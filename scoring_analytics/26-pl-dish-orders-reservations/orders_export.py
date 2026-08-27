"""
Export single-market platform Order and Reservation rows to the partner
event bus.

Streams BigQuery rows → Avro → chunked POST /ingestbulk/{country}/{schemaId}.
One shared sender covers both contracts; only the SELECT, Avro schema,
schema id, and row mapper differ.

Source (read-only):
  dags/horeca_digital/dana_pl_dish_orders_export.py

Sanitized vs production:
  - Brand / project / table / API host names generalized
  - Schema ids + ingest base externalized to Variables
  - Local `batched` helper (was horeca_digital.utils.dsa_cost.batched)
  - Avro schema parsed once per send
  - 401 clears token and retries the same payload
  - Owner contact fields kept in the contract (partner requires them);
    treat as sensitive in ops even if the registered schema marks
    table_PII=no
"""

from __future__ import annotations

import base64
import io
import json
import logging
import os
import time
from datetime import datetime
from typing import Callable, Iterable, Iterator, List, TypeVar

import avro.io
import avro.schema
import requests
from airflow.models import Variable
from google.cloud import bigquery

from orders_query import MarketDishOrders

T = TypeVar("T")

ENV_VAR_NAME = "env"
ENV = os.environ.get(ENV_VAR_NAME, Variable.get(ENV_VAR_NAME))

if ENV == "DEV":
    OAUTH_USERNAME = Variable.get("event_api_username")
    OAUTH_PASSWORD = Variable.get("event_api_password")
    CLIENT_ID = Variable.get("event_api_client_id_dev")
    CLIENT_SECRET = Variable.get("event_api_client_secret_dev")
    OAUTH2_URL = Variable.get("event_api_oauth2_url_dev")
    BIGQUERY_PROJECT = "dwh_project_dev"
    ORDERS_SCHEMA_ID = Variable.get(
        "market_dish_orders_schema_id_dev",
        default_var="MARKET_DISH_ORDERS_SCHEMA_DEV",
    )
    RESERVATIONS_SCHEMA_ID = Variable.get(
        "market_dish_reservations_schema_id_dev",
        default_var="MARKET_DISH_RESERVATIONS_SCHEMA_DEV",
    )
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
    ORDERS_SCHEMA_ID = Variable.get(
        "market_dish_orders_schema_id",
        default_var="MARKET_DISH_ORDERS_SCHEMA_PROD",
    )
    RESERVATIONS_SCHEMA_ID = Variable.get(
        "market_dish_reservations_schema_id",
        default_var="MARKET_DISH_RESERVATIONS_SCHEMA_PROD",
    )
    INGEST_BASE = Variable.get(
        "event_api_ingest_base",
        default_var="https://api.example.com/event-ingest/bulk",
    )

CHUNK_SIZE = 500

ORDERS_AVRO_SCHEMA = json.dumps(
    {
        "namespace": "company",
        "type": "record",
        "name": "market_dish_orders",
        "doc": "Single-market platform Order lifetime extract for partner ingest",
        "gdpr_info": {"table_PII": "no", "column_PII": []},
        "fields": [
            {"name": "establishment_id", "type": ["null", "string"], "default": None},
            {"name": "establishment_name", "type": ["null", "string"], "default": None},
            {"name": "wholesale_id", "type": ["null", "string"], "default": None},
            {"name": "store_id", "type": ["null", "string"], "default": None},
            {"name": "postalcode", "type": ["null", "string"], "default": None},
            {"name": "city", "type": ["null", "string"], "default": None},
            {"name": "owner_first_name", "type": ["null", "string"], "default": None},
            {"name": "owner_email", "type": ["null", "string"], "default": None},
            {"name": "owner_last_name", "type": ["null", "string"], "default": None},
            {"name": "owner_phone", "type": ["null", "string"], "default": None},
            {"name": "product_code", "type": ["null", "string"], "default": None},
            {"name": "commitment_period", "type": ["null", "long"], "default": None},
            {"name": "asset_status", "type": ["null", "string"], "default": None},
            {"name": "asset_referrer", "type": ["null", "string"], "default": None},
            {"name": "order_id", "type": ["null", "long"], "default": None},
            {"name": "order_number", "type": ["null", "string"], "default": None},
            {"name": "order_date", "type": ["null", "string"], "default": None},
            {"name": "order_status", "type": ["null", "string"], "default": None},
            {"name": "order_price", "type": ["null", "double"], "default": None},
        ],
    }
)

RESERVATIONS_AVRO_SCHEMA = json.dumps(
    {
        "namespace": "company",
        "type": "record",
        "name": "market_dish_reservations",
        "doc": "Single-market platform Reservation lifetime extract for partner ingest",
        "gdpr_info": {"table_PII": "no", "column_PII": []},
        "fields": [
            {"name": "establishment_id", "type": ["null", "string"], "default": None},
            {"name": "establishment_name", "type": ["null", "string"], "default": None},
            {"name": "wholesale_id", "type": ["null", "string"], "default": None},
            {"name": "store_id", "type": ["null", "string"], "default": None},
            {"name": "postalcode", "type": ["null", "string"], "default": None},
            {"name": "city", "type": ["null", "string"], "default": None},
            {"name": "owner_first_name", "type": ["null", "string"], "default": None},
            {"name": "owner_last_name", "type": ["null", "string"], "default": None},
            {"name": "owner_email", "type": ["null", "string"], "default": None},
            {"name": "owner_phone", "type": ["null", "string"], "default": None},
            {"name": "product_code", "type": ["null", "string"], "default": None},
            {"name": "commitment_period", "type": ["null", "long"], "default": None},
            {"name": "asset_status", "type": ["null", "string"], "default": None},
            {"name": "asset_referrer", "type": ["null", "string"], "default": None},
            {"name": "reservation_id", "type": ["null", "long"], "default": None},
            {
                "name": "reservation_created_date",
                "type": ["null", "string"],
                "default": None,
            },
        ],
    }
)


def batched(iterable: Iterable[T], n: int) -> Iterator[List[T]]:
    """Yield lists of up to n items from iterable (streaming-friendly)."""
    if n < 1:
        raise ValueError("n must be >= 1")
    batch: List[T] = []
    for item in iterable:
        batch.append(item)
        if len(batch) >= n:
            yield batch
            batch = []
    if batch:
        yield batch


def _b64(s: str) -> str:
    return base64.b64encode(s.encode()).decode()


def _long(d: dict, k: str):
    v = d.get(k)
    return int(v) if v is not None else None


def _double(d: dict, k: str):
    v = d.get(k)
    return float(v) if v is not None else None


def _str(d: dict, k: str):
    v = d.get(k)
    return (v if isinstance(v, str) else str(v)) if v is not None else None


class EventAPI:
    def __init__(self, token_url: str, client_id: str, client_secret: str):
        self.token_url = token_url
        self.token = None
        self.client_id = client_id
        self.client_secret = client_secret

    def get_token(self):
        headers = {
            "Authorization": "Basic " + _b64(self.client_id + ":" + self.client_secret),
            "Content-Type": "application/x-www-form-urlencoded",
        }
        params = {
            "grant_type": "password",
            "username": OAUTH_USERNAME,
            "password": OAUTH_PASSWORD,
        }
        r = requests.post(self.token_url, headers=headers, params=params)
        r.raise_for_status()
        self.token = r.json()["access_token"]
        return self.token

    def endpoint(self, url: str, data=None, max_retries: int = 10):
        if self.token is None:
            self.get_token()
        headers = {
            "Authorization": "Bearer " + self.token,
            "Content-Type": "application/vnd.company.events.json",
        }
        for attempt in range(max_retries):
            try:
                r = requests.post(url, headers=headers, data=data, timeout=120)
                if r.status_code == 401:
                    self.token = None
                    self.get_token()
                    headers["Authorization"] = "Bearer " + self.token
                    continue
                r.raise_for_status()
                return r.json()
            except (
                requests.exceptions.JSONDecodeError,
                requests.exceptions.RequestException,
            ) as e:
                wait = min(5 * (attempt + 1), 60)
                logging.warning(
                    "Event API attempt %d/%d failed: %s — retrying in %ds",
                    attempt + 1,
                    max_retries,
                    e,
                    wait,
                )
                if attempt < max_retries - 1:
                    time.sleep(wait)
                else:
                    raise


def _order_row_to_avro(row) -> dict:
    d = dict(row.items())
    return {
        "establishment_id": _str(d, "establishment_id"),
        "establishment_name": _str(d, "establishment_name"),
        "wholesale_id": _str(d, "wholesale_id"),
        "store_id": _str(d, "store_id"),
        "postalcode": _str(d, "postalcode"),
        "city": _str(d, "city"),
        "owner_first_name": _str(d, "owner_first_name"),
        "owner_email": _str(d, "owner_email"),
        "owner_last_name": _str(d, "owner_last_name"),
        "owner_phone": _str(d, "owner_phone"),
        "product_code": _str(d, "product_code"),
        "commitment_period": _long(d, "commitment_period"),
        "asset_status": _str(d, "asset_status"),
        "asset_referrer": _str(d, "asset_referrer"),
        "order_id": _long(d, "order_id"),
        "order_number": _str(d, "order_number"),
        "order_date": _str(d, "order_date"),
        "order_status": _str(d, "order_status"),
        "order_price": _double(d, "order_price"),
    }


def _reservation_row_to_avro(row) -> dict:
    d = dict(row.items())
    return {
        "establishment_id": _str(d, "establishment_id"),
        "establishment_name": _str(d, "establishment_name"),
        "wholesale_id": _str(d, "wholesale_id"),
        "store_id": _str(d, "store_id"),
        "postalcode": _str(d, "postalcode"),
        "city": _str(d, "city"),
        "owner_first_name": _str(d, "owner_first_name"),
        "owner_last_name": _str(d, "owner_last_name"),
        "owner_email": _str(d, "owner_email"),
        "owner_phone": _str(d, "owner_phone"),
        "product_code": _str(d, "product_code"),
        "commitment_period": _long(d, "commitment_period"),
        "asset_status": _str(d, "asset_status"),
        "asset_referrer": _str(d, "asset_referrer"),
        "reservation_id": _long(d, "reservation_id"),
        "reservation_created_date": _str(d, "reservation_created_date"),
    }


def _encode_row(row, writer, row_to_avro: Callable) -> str:
    bw = io.BytesIO()
    encoder = avro.io.BinaryEncoder(bw)
    writer.write(row_to_avro(row), encoder)
    return base64.b64encode(bw.getvalue()).decode("utf-8")


def _ingestbulk_url(country: str, schema_id: str) -> str:
    return f"{INGEST_BASE.rstrip('/')}/{country.lower()}/{schema_id}"


def _send_dataset(country, query, schema_json, schema_id, row_to_avro, label):
    api = EventAPI(OAUTH2_URL, CLIENT_ID, CLIENT_SECRET)
    print(f"{datetime.now()} - [{country}] {label} querying BQ...")
    client = bigquery.Client(project=BIGQUERY_PROJECT)
    results = client.query(query).result()
    base_url = _ingestbulk_url(country, schema_id)

    schema_parsed = avro.schema.parse(schema_json)
    writer = avro.io.DatumWriter(schema_parsed)

    row_count = 0
    encoded = (_encode_row(row, writer, row_to_avro) for row in results)
    for chunk in batched(encoded, CHUNK_SIZE):
        payload = json.dumps({"records": [{"value": rec} for rec in chunk]})
        resp = api.endpoint(url=base_url, data=payload)
        row_count += len(chunk)
        print(f"{datetime.now()} - [{country}] {label} rows {row_count} — {resp}")

    print(f"{datetime.now()} - [{country}] {label} DONE. Total rows: {row_count}")


def send_market_dish_orders_data(country: str = "pl"):
    """Full-load export of market Order rows to partner ingestbulk."""
    _send_dataset(
        country=country,
        query=MarketDishOrders.get_orders_query(),
        schema_json=ORDERS_AVRO_SCHEMA,
        schema_id=ORDERS_SCHEMA_ID,
        row_to_avro=_order_row_to_avro,
        label="market_dish_orders",
    )


def send_market_dish_reservations_data(country: str = "pl"):
    """Full-load export of market Reservation rows to partner ingestbulk."""
    _send_dataset(
        country=country,
        query=MarketDishOrders.get_reservations_query(),
        schema_json=RESERVATIONS_AVRO_SCHEMA,
        schema_id=RESERVATIONS_SCHEMA_ID,
        row_to_avro=_reservation_row_to_avro,
        label="market_dish_reservations",
    )


if __name__ == "__main__":
    # Local smoke: parse modules / schemas only — no network.
    avro.schema.parse(ORDERS_AVRO_SCHEMA)
    avro.schema.parse(RESERVATIONS_AVRO_SCHEMA)
    print("schemas ok; call send_* from Composer with Variables set")
