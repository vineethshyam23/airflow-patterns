"""
Avro bulk ingest for ranked menu-gap opportunities.

Streams BigQuery rows for one FARM_FINGERPRINT partition, Avro-encodes
them, and POSTs chunks to /ingestbulk/{country}/{schema_id}.

Concurrency contract (owned by the DAG):
  - countries run sequentially
  - within a country, TOTAL_BATCHES parallel tasks split rows via
    MOD(ABS(FARM_FINGERPRINT(establishment_id || article_no)), N)

Credentials and schema ids come from Airflow Variables only.

Source (read-only):
  dags/horeca_digital/dana_rex_menu_gaps_export.py
  dags/horeca_digital/dana_rex_menu_gaps_query.py

Sanitized fixes vs production module:
  - Schema id / ingest base externalized to Variables
  - GCP project / dataset / company names generalized
  - Column names de-branded (metro_* → wholesale_*/customer_*)
  - 401 retry keeps the original payload (already true in source;
    left explicit here)
  - Avro schema parsed once per batch (source already did this)
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
        "menu_gaps_ranked_schema_id_dev", default_var="MENU_GAPS_SCHEMA_DEV"
    )
    SCHEMA_NAME = "menu_gaps_ranked_dev"
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
        "menu_gaps_ranked_schema_id", default_var="MENU_GAPS_SCHEMA_PROD"
    )
    SCHEMA_NAME = "menu_gaps_ranked"
    INGEST_BASE = Variable.get(
        "event_api_ingest_base",
        default_var="https://api.example.com/event-ingest/bulk",
    )


MENU_GAPS_AVRO_SCHEMA = json.dumps(
    {
        "namespace": "company",
        "type": "record",
        "name": SCHEMA_NAME,
        "doc": "Ranked menu-gap opportunities per establishment (refined)",
        "gdpr_info": {"table_PII": "no", "column_PII": []},
        "fields": [
            {"name": "wholesale_id", "type": ["null", "long"], "default": None},
            {"name": "iso_code", "type": ["null", "string"], "default": None},
            {"name": "establishment_id", "type": ["null", "string"], "default": None},
            {"name": "ingredient", "type": ["null", "string"], "default": None},
            {"name": "type", "type": ["null", "string"], "default": None},
            {"name": "menu_type", "type": ["null", "string"], "default": None},
            {"name": "menu_item_name", "type": ["null", "string"], "default": None},
            {"name": "relevance", "type": ["null", "double"], "default": None},
            {"name": "branch_desc", "type": ["null", "string"], "default": None},
            {"name": "article_no", "type": ["null", "long"], "default": None},
            {"name": "variant_tu_key", "type": ["null", "long"], "default": None},
            {"name": "department_flag", "type": ["null", "string"], "default": None},
            {"name": "product_key", "type": ["null", "long"], "default": None},
            {"name": "article_name", "type": ["null", "string"], "default": None},
            {"name": "one_year_revenue", "type": ["null", "double"], "default": None},
            {"name": "rank_", "type": ["null", "long"], "default": None},
            {"name": "account_id", "type": ["null", "string"], "default": None},
            {"name": "person_id", "type": ["null", "string"], "default": None},
            {"name": "cardholder_key", "type": ["null", "long"], "default": None},
            {"name": "customer_key", "type": ["null", "long"], "default": None},
            {"name": "unique_wholesale_id", "type": ["null", "long"], "default": None},
            {"name": "_update_ts", "type": ["null", "string"], "default": None},
        ],
    }
)

CHUNK_SIZE = 2000
COUNTRY_ISO_CODES = ["de", "fr", "nl", "es", "pl", "hr", "it", "pt"]

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
                    # Keep original payload — older siblings dropped it on retry.
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

    def _long(k: str) -> Optional[int]:
        v = d.get(k)
        return int(v) if v is not None else None

    def _double(k: str) -> Optional[float]:
        v = d.get(k)
        return float(v) if v is not None else None

    def _str(k: str) -> Optional[str]:
        v = d.get(k)
        if v is None:
            return None
        return v if isinstance(v, str) else str(v)

    return {
        "wholesale_id": _long("wholesale_id"),
        "iso_code": _str("iso_code"),
        "establishment_id": _str("establishment_id"),
        "ingredient": _str("ingredient"),
        "type": _str("type"),
        "menu_type": _str("menu_type"),
        "menu_item_name": _str("menu_item_name"),
        "relevance": _double("relevance"),
        "branch_desc": _str("branch_desc"),
        "article_no": _long("article_no"),
        "variant_tu_key": _long("variant_tu_key"),
        "department_flag": _str("department_flag"),
        "product_key": _long("product_key"),
        "article_name": _str("article_name"),
        "one_year_revenue": _double("one_year_revenue"),
        "rank_": _long("rank_"),
        "account_id": _str("account_id"),
        "person_id": _str("person_id"),
        "cardholder_key": _long("cardholder_key"),
        "customer_key": _long("customer_key"),
        "unique_wholesale_id": _long("unique_wholesale_id"),
        "_update_ts": _str("_update_ts"),
    }


def _build_query(
    iso_code_lower: str,
    batch_number: int,
    total_batches: int,
    full_load: bool = False,
) -> str:
    """
    Partition key: establishment_id + article_no.

    FARM_FINGERPRINT MOD N gives disjoint, roughly even slices without
    a precomputed batch column. Same key must be used for every run or
    rows migrate between batches on reprocess.
    """
    cc = iso_code_lower.lower()
    query = f"""
    SELECT
        wholesale_id, iso_code, establishment_id, ingredient, `type`, menu_type,
        menu_item_name, relevance, branch_desc, article_no, variant_tu_key,
        department_flag, product_key, article_name, one_year_revenue, rank_,
        account_id, person_id, cardholder_key, customer_key, unique_wholesale_id,
        FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%E*S', _update_ts) AS _update_ts
    FROM `refined.menu_gaps_ranked_{cc}`
    WHERE MOD(
        ABS(FARM_FINGERPRINT(CONCAT(
            CAST(establishment_id AS STRING), '-', CAST(article_no AS STRING)
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
    country table in memory. Chunk size 2000 is intentional: menu-gap
    rows are wider than KYC/matching siblings (chunk 500) but still
    fit comfortably under typical API body limits.
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
    print(_build_query("de", 0, 5)[:320])
    print(f"{datetime.utcnow().isoformat()}Z smoke done")
