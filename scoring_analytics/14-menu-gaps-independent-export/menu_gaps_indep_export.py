"""
Avro bulk ingest for independent-establishment menu-gap opportunities.

Streams BigQuery rows for one FARM_FINGERPRINT partition, Avro-encodes
them, and POSTs chunks to /ingestbulk/{country}/{schema_id}.

Concurrency contract (owned by the DAG):
  - countries run sequentially
  - within a country, TOTAL_BATCHES parallel tasks split rows via
    MOD(ABS(FARM_FINGERPRINT(establishment_id || menu_item || ingredient)), N)

Distinct from pattern 12:
  - Schema is address / geo / contact + gap fields (not ranked articles)
  - Partition key uses menu_item_name + ingredient (no article_no)
  - Chunk size 1000 (narrower payload shape than ranked rows at 2000)
  - No dbt step in the export DAG — refined tables are upstream

Credentials and schema ids come from Airflow Variables only.

Source (read-only):
  dags/horeca_digital/dana_rex_menu_gaps_non_metro_export.py
  dags/etl_dana_rex_menu_gaps_non_metro_export.py
"""

from __future__ import annotations

import base64
import io
import json
import logging
import os
import time
from datetime import datetime
from typing import Any, Dict, Optional

import avro.io
import avro.schema
import requests
from airflow.models import Variable
from google.cloud import bigquery

from menu_gaps_indep_query import ACTIVE_ISO_CODES

ENV_VAR_NAME = "env"
ENV = os.environ.get(ENV_VAR_NAME, Variable.get(ENV_VAR_NAME))

if ENV == "DEV":
    OAUTH_USERNAME = Variable.get("event_api_username")
    OAUTH_PASSWORD = Variable.get("event_api_password")
    CLIENT_ID = Variable.get("event_api_client_id_dev")
    CLIENT_SECRET = Variable.get("event_api_client_secret_dev")
    OAUTH2_URL = Variable.get("event_api_oauth2_url_dev")
    BIGQUERY_PROJECT = "dwh_project_dev"
    MENU_GAPS_SCHEMA_ID = Variable.get(
        "menu_gaps_independent_schema_id_dev",
        default_var="MENU_GAPS_INDEP_SCHEMA_DEV",
    )
    SCHEMA_NAME = "menu_gaps_independent_dev"
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
    MENU_GAPS_SCHEMA_ID = Variable.get(
        "menu_gaps_independent_schema_id",
        default_var="MENU_GAPS_INDEP_SCHEMA_PROD",
    )
    SCHEMA_NAME = "menu_gaps_independent"
    INGEST_BASE = Variable.get(
        "event_api_ingest_base",
        default_var="https://api.example.com/event-ingest/bulk",
    )


MENU_GAPS_AVRO_SCHEMA = json.dumps(
    {
        "namespace": "company",
        "type": "record",
        "name": SCHEMA_NAME,
        "doc": "Menu-gap opportunities for independent establishments per country",
        "gdpr_info": {
            "table_PII": "yes",
            "column_PII": ["phone", "email", "address", "geo_lat", "geo_long"],
        },
        "fields": [
            {"name": "establishment_id", "type": ["null", "string"], "default": None},
            {"name": "iso_code", "type": ["null", "string"], "default": None},
            {"name": "establishment_name", "type": ["null", "string"], "default": None},
            {"name": "postal_code", "type": ["null", "string"], "default": None},
            {"name": "city", "type": ["null", "string"], "default": None},
            {"name": "street_name", "type": ["null", "string"], "default": None},
            {"name": "street_number", "type": ["null", "string"], "default": None},
            {"name": "address", "type": ["null", "string"], "default": None},
            {"name": "geo_lat", "type": ["null", "double"], "default": None},
            {"name": "geo_long", "type": ["null", "double"], "default": None},
            {"name": "google_places_id", "type": ["null", "string"], "default": None},
            {"name": "phone", "type": ["null", "string"], "default": None},
            {"name": "email", "type": ["null", "string"], "default": None},
            {"name": "website", "type": ["null", "string"], "default": None},
            {"name": "establishment_type", "type": ["null", "string"], "default": None},
            {"name": "cuisine_type", "type": ["null", "string"], "default": None},
            {"name": "menu_type", "type": ["null", "string"], "default": None},
            {"name": "menu_item_name", "type": ["null", "string"], "default": None},
            {"name": "ingredient", "type": ["null", "string"], "default": None},
            {"name": "created_at", "type": ["null", "string"], "default": None},
            {"name": "_update_ts", "type": ["null", "string"], "default": None},
        ],
    }
)

CHUNK_SIZE = 1000
COUNTRY_ISO_CODES = list(ACTIVE_ISO_CODES)

log = logging.getLogger(__name__)


def _b64(s: str) -> str:
    return base64.b64encode(s.encode()).decode()


class EventApiClient:
    """OAuth password-grant client with retry on transient failures."""

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
        r = requests.post(self.token_url, headers=headers, params=params, timeout=60)
        r.raise_for_status()
        self.token = r.json()["access_token"]
        return self.token

    def post_bulk(self, url: str, data: str, max_retries: int = 10) -> Any:
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
                    # Keep original payload — retry the same body after refresh.
                    self.token = None
                    self.get_token()
                    headers["Authorization"] = "Bearer " + self.token
                    continue
                r.raise_for_status()
                return r.json()
            except (requests.exceptions.JSONDecodeError, requests.exceptions.RequestException) as e:
                wait = min(5 * (attempt + 1), 60)
                log.warning(
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


def _row_to_avro_dict(row: Any) -> Dict[str, Any]:
    d = dict(row.items())

    def _double(k: str) -> Optional[float]:
        v = d.get(k)
        return float(v) if v is not None else None

    def _str(k: str) -> Optional[str]:
        v = d.get(k)
        if v is None:
            return None
        return v if isinstance(v, str) else str(v)

    return {
        "establishment_id": _str("establishment_id"),
        "iso_code": _str("iso_code"),
        "establishment_name": _str("establishment_name"),
        "postal_code": _str("postal_code"),
        "city": _str("city"),
        "street_name": _str("street_name"),
        "street_number": _str("street_number"),
        "address": _str("address"),
        "geo_lat": _double("geo_lat"),
        "geo_long": _double("geo_long"),
        "google_places_id": _str("google_places_id"),
        "phone": _str("phone"),
        "email": _str("email"),
        "website": _str("website"),
        "establishment_type": _str("establishment_type"),
        "cuisine_type": _str("cuisine_type"),
        "menu_type": _str("menu_type"),
        "menu_item_name": _str("menu_item_name"),
        "ingredient": _str("ingredient"),
        "created_at": _str("created_at"),
        "_update_ts": _str("_update_ts"),
    }


def _build_query(
    iso_code_lower: str,
    batch_number: int,
    total_batches: int,
    full_load: bool = False,
) -> str:
    """
    Partition key: establishment_id + menu_item_name + ingredient.

    No article_no on this feed — the natural grain is the gap itself.
    FARM_FINGERPRINT MOD N gives disjoint slices without a batch column.
    """
    cc = iso_code_lower.lower()
    query = f"""
    SELECT
        establishment_id,
        iso_code,
        establishment_name,
        postal_code,
        city,
        street_name,
        street_number,
        address,
        geo_lat,
        geo_long,
        google_places_id,
        phone,
        email,
        website,
        establishment_type,
        cuisine_type,
        menu_type,
        menu_item_name,
        ingredient,
        CAST(created_at AS STRING) AS created_at,
        FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%E*S', _update_ts) AS _update_ts
    FROM `refined.menu_gaps_independent_{cc}`
    WHERE MOD(
        ABS(FARM_FINGERPRINT(CONCAT(
            CAST(establishment_id AS STRING), '-',
            IFNULL(menu_item_name, ''), '-',
            IFNULL(ingredient, '')
        ))),
        {total_batches}
    ) = {batch_number}
    """
    if not full_load:
        query += """
        AND _update_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
        """
    return query


def send_ranged_batch(
    iso_code_lower: str,
    batch_number: int,
    total_batches: int,
    full_load: bool = False,
) -> None:
    """
    Export one hash partition for a country.

    Streams the BQ result iterator — do not materialize the full
    country table in memory. Chunk 1000 matches the production
    independent-gaps module (narrower than ranked rows at 2000).
    """
    client_api = EventApiClient(OAUTH2_URL, CLIENT_ID, CLIENT_SECRET)
    query = _build_query(iso_code_lower, batch_number, total_batches, full_load=full_load)

    log.info(
        "[%s] batch %d/%d querying BQ (full_load=%s)...",
        iso_code_lower,
        batch_number,
        total_batches,
        full_load,
    )
    bq = bigquery.Client(project=BIGQUERY_PROJECT)
    results = bq.query(query).result()

    base_url = f"{INGEST_BASE.rstrip('/')}/{iso_code_lower.lower()}/{MENU_GAPS_SCHEMA_ID}"
    schema_parsed = avro.schema.parse(MENU_GAPS_AVRO_SCHEMA)
    writer = avro.io.DatumWriter(schema_parsed)

    chunk_buf = []
    row_count = 0

    for row in results:
        bw = io.BytesIO()
        encoder = avro.io.BinaryEncoder(bw)
        writer.write(_row_to_avro_dict(row), encoder)
        chunk_buf.append(base64.b64encode(bw.getvalue()).decode("utf-8"))
        row_count += 1

        if len(chunk_buf) >= CHUNK_SIZE:
            payload = json.dumps({"records": [{"value": rec} for rec in chunk_buf]})
            resp = client_api.post_bulk(url=base_url, data=payload)
            log.info(
                "[%s] batch %d rows %d — %s",
                iso_code_lower,
                batch_number,
                row_count,
                resp,
            )
            chunk_buf = []

    if chunk_buf:
        payload = json.dumps({"records": [{"value": rec} for rec in chunk_buf]})
        resp = client_api.post_bulk(url=base_url, data=payload)
        log.info(
            "[%s] batch %d rows %d — %s",
            iso_code_lower,
            batch_number,
            row_count,
            resp,
        )

    log.info(
        "[%s] batch %d/%d DONE. Total rows: %d",
        iso_code_lower,
        batch_number,
        total_batches,
        row_count,
    )


if __name__ == "__main__":
    # Smoke: schema parse + query shape. No network / BQ calls.
    parsed = avro.schema.parse(MENU_GAPS_AVRO_SCHEMA)
    print(f"schema ok: {parsed.name}, fields={len(parsed.fields)}")
    print(_build_query("es", 0, 5)[:360])
    print(f"{datetime.utcnow().isoformat()}Z smoke done")
