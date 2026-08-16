"""
Monthly full-load Avro export of establishment market data.

Streams one FARM_FINGERPRINT shard of refined.establishment_market_data_{cc}
to a partner event ingest API. The DAG owns country order and batch
fan-out; this module owns OAuth + encode + POST.

Why full load monthly (not hash-delta):
  Market listing attributes churn across many columns (hours, ratings,
  social, topics). A stable hash of the whole row is brittle; the
  partner preferred a deterministic monthly reship of the country
  table. Cost is higher than a delta, but ops stays simple.

Source (read-only):
  dags/horeca_digital/dana_dish_market_data_export.py
  dags/horeca_digital/foodgraph_queries.py (active ISO list)

Sanitized vs production:
  - GCP project / dataset / schema names generalized
  - Event API host + schema ids externalized to Variables
  - Real OAuth Variable names generalized
  - Package import horeca_digital.* → local countries module
  - Schema id no longer hard-coded prod hex
"""

from __future__ import annotations

import base64
import io
import json
import logging
import os
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

import avro.io
import avro.schema
import requests
from airflow.models import Variable
from google.cloud import bigquery

from countries import ACTIVE_ISO_CODES

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
        "establishment_market_data_schema_id_dev",
        default_var="MARKET_DATA_SCHEMA_DEV",
    )
    SCHEMA_NAME = "establishment_market_data_dev"
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
        "establishment_market_data_schema_id",
        default_var="MARKET_DATA_SCHEMA_PROD",
    )
    SCHEMA_NAME = "establishment_market_data"
    INGEST_BASE = Variable.get(
        "event_api_ingest_base",
        default_var="https://api.example.com/event-ingest/bulk",
    )

# geo_lat / geo_long stay doubles; everything else ships as nullable string.
# JSON-typed BQ columns (attributes, topics, …) are stringified in Python —
# BigQuery forbids CAST(JSON AS STRING) in the SELECT.
_DOUBLE_FIELDS = ["geo_lat", "geo_long"]

FIELD_ORDER = [
    "restaurant_name",
    "original_title",
    "establishment_id",
    "places_id",
    "street_name",
    "postal_code",
    "city",
    "region",
    "country",
    "geo_lat",
    "geo_long",
    "phones",
    "emails",
    "website",
    "domain",
    "is_closed",
    "is_claimed",
    "open_hours",
    "first_seen",
    "rating_google",
    "rating_google_n",
    "rating_distribution",
    "price_range",
    "has_delivery",
    "has_delivery_takeaway",
    "has_online_reservation",
    "establishment_type",
    "cuisine_type",
    "primary_category_id",
    "category_ids",
    "menu_url",
    "check_url",
    "has_facebook",
    "page_facebook",
    "has_instagram",
    "page_instagram",
    "description",
    "attributes",
    "attributes_available_payments",
    "place_topics",
    "popular_times",
    "people_also_search",
    "created_at",
    "_update_ts",
]

def _build_avro_schema(schema_name: str) -> str:
    fields = []
    for name in FIELD_ORDER:
        avro_type = "double" if name in _DOUBLE_FIELDS else "string"
        fields.append(
            '    {"name": "%s", "type": ["null", "%s"], "default": null}'
            % (name, avro_type)
        )
    return (
        "{\n"
        '  "namespace": "company",\n'
        '  "type": "record",\n'
        '  "name": "%s",\n'
        '  "doc": "Establishment market listing extract per country",\n'
        '  "gdpr_info": {"table_PII": "no", "column_PII": []},\n'
        '  "fields": [\n%s\n  ]\n'
        "}"
    ) % (schema_name, ",\n".join(fields))


MARKET_DATA_AVRO_SCHEMA = _build_avro_schema(SCHEMA_NAME)

CHUNK_SIZE = 1000
COUNTRY_ISO_CODES = list(ACTIVE_ISO_CODES)


def _b64(s: str) -> str:
    return base64.b64encode(s.encode()).decode()


class EventApiClient:
    """Password-grant OAuth + POST with linear backoff and 401 refresh."""

    def __init__(self, token_url: str, client_id: str, client_secret: str):
        self.token_url = token_url
        self.client_id = client_id
        self.client_secret = client_secret
        self.token: Optional[str] = None

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
        r = requests.post(self.token_url, headers=headers, params=params)
        r.raise_for_status()
        self.token = r.json()["access_token"]
        return self.token

    def post_json(self, url: str, data: Optional[str] = None, max_retries: int = 10):
        if self.token is None:
            self.get_token()
        headers = {
            "Authorization": "Bearer " + self.token,
            "Content-Type": "application/vnd.company.event.events.json",
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
            except (requests.exceptions.JSONDecodeError, requests.exceptions.RequestException) as e:
                wait = min(5 * (attempt + 1), 60)
                logging.warning(
                    "event API attempt %d/%d failed: %s — retrying in %ds",
                    attempt + 1,
                    max_retries,
                    e,
                    wait,
                )
                if attempt < max_retries - 1:
                    time.sleep(wait)
                else:
                    raise


def _row_to_avro_dict(row) -> Dict[str, Any]:
    raw = dict(row.items())

    def _double(key: str):
        v = raw.get(key)
        return float(v) if v is not None else None

    def _str(key: str):
        v = raw.get(key)
        if v is None:
            return None
        if isinstance(v, str):
            return v
        # JSON / STRUCT / ARRAY columns — serialize; do not CAST in SQL.
        if isinstance(v, (dict, list)):
            return json.dumps(v, ensure_ascii=False, default=str)
        return str(v)

    out: Dict[str, Any] = {}
    for name in FIELD_ORDER:
        out[name] = _double(name) if name in _DOUBLE_FIELDS else _str(name)
    return out


def _build_query(iso_code_lower: str, batch_number: int, total_batches: int) -> str:
    """Full monthly load: whole country table, sharded by establishment_id."""
    cc = iso_code_lower.lower()
    select_cols: List[str] = []
    for name in FIELD_ORDER:
        if name in _DOUBLE_FIELDS:
            select_cols.append(f"CAST({name} AS FLOAT64) AS {name}")
        elif name == "_update_ts":
            select_cols.append(
                "FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%E*S', _update_ts) AS _update_ts"
            )
        else:
            # Select raw — JSON-typed columns cannot CAST AS STRING in BQ.
            select_cols.append(name)
    select_clause = ",\n        ".join(select_cols)

    return f"""
    SELECT
        {select_clause}
    FROM `refined.establishment_market_data_{cc}`
    WHERE MOD(
        ABS(FARM_FINGERPRINT(CAST(establishment_id AS STRING))),
        {total_batches}
    ) = {batch_number}
    """

def send_ranged_batch(iso_code_lower: str, batch_number: int, total_batches: int):
    """Export one hash partition for a country to event ingest."""
    client = EventApiClient(OAUTH2_URL, CLIENT_ID, CLIENT_SECRET)
    query = _build_query(iso_code_lower, batch_number, total_batches)

    print(
        f"{datetime.now()} - [{iso_code_lower}] batch "
        f"{batch_number}/{total_batches} querying BQ..."
    )
    bq = bigquery.Client(project=BIGQUERY_PROJECT)
    results = bq.query(query).result()

    base_url = f"{INGEST_BASE.rstrip('/')}/{iso_code_lower.lower()}/{SCHEMA_ID}"
    schema_parsed = avro.schema.parse(MARKET_DATA_AVRO_SCHEMA)
    writer = avro.io.DatumWriter(schema_parsed)

    chunk_buf: List[str] = []
    row_count = 0

    for row in results:
        bw = io.BytesIO()
        encoder = avro.io.BinaryEncoder(bw)
        writer.write(_row_to_avro_dict(row), encoder)
        chunk_buf.append(base64.b64encode(bw.getvalue()).decode("utf-8"))
        row_count += 1

        if len(chunk_buf) >= CHUNK_SIZE:
            payload = json.dumps({"records": [{"value": rec} for rec in chunk_buf]})
            resp = client.post_json(url=base_url, data=payload)
            print(
                f"{datetime.now()} - [{iso_code_lower}] batch {batch_number} "
                f"rows {row_count} — {resp}"
            )
            chunk_buf = []

    if chunk_buf:
        payload = json.dumps({"records": [{"value": rec} for rec in chunk_buf]})
        resp = client.post_json(url=base_url, data=payload)
        print(
            f"{datetime.now()} - [{iso_code_lower}] batch {batch_number} "
            f"rows {row_count} — {resp}"
        )

    print(
        f"{datetime.now()} - [{iso_code_lower}] batch "
        f"{batch_number}/{total_batches} DONE. Total rows: {row_count}"
    )


if __name__ == "__main__":
    # Smoke: schema parse + one query prefix. No network.
    parsed = avro.schema.parse(MARKET_DATA_AVRO_SCHEMA)
    print("avro fields:", len(parsed.fields), "countries:", COUNTRY_ISO_CODES)
    print(_build_query("es", 0, 5)[:280])
