"""
Monthly full-load Avro export of platform POS revenue potentials.

Subject: platform_pos_revenue
Grain: wholesale customer (country_code, customer_no, home_store_id)

One full load per country (DE, FR, ES, IT). The extract matches the
SAM / pos_potential annual_sale contract: trailing-12-month net POS
revenue, annualised when the licence has been live under a year and
is not suspended. Country is the ingest path, not part of the subject.

Streams BigQuery → Avro (500-row chunks) → partner bulk ingest.
OAuth prefers client_credentials, then password-grant fallback.

Source (read-only):
  dags/etl_dana_pos_revenue_export.py
  dags/horeca_digital/dana_pos_revenue_export.py

Sanitized vs production:
  - GCP projects / datasets / tables generalized
  - Event API host + schema ids externalized to Variables
  - Brand / ticket / owner identifiers removed
  - metro_* → wholesale_*; cust_no → customer_no
  - Package import horeca_digital.utils.dsa_cost.batched → local batched
"""

from __future__ import annotations

import base64
import io
import json
import logging
import os
import time
from datetime import datetime
from typing import Iterable, Iterator, List, Optional, Tuple, TypeVar

import avro.io
import avro.schema
import requests
from airflow.models import Variable
from google.cloud import bigquery

T = TypeVar("T")

ENV_VAR_NAME = "env"
ENV = os.environ.get(ENV_VAR_NAME, Variable.get(ENV_VAR_NAME))

SOURCE_PROJECT = "dwh_project"
COUNTRY_ISO_CODES = ["de", "fr", "es", "it"]
CHUNK_SIZE = 500
PROD_SCHEMA_ID_DEFAULT = "POS_REVENUE_SCHEMA_PROD"

if ENV == "DEV":
    OAUTH_USERNAME = Variable.get("event_api_username")
    OAUTH_PASSWORD = Variable.get("event_api_password")
    CLIENT_ID = Variable.get("event_api_client_id_dev")
    CLIENT_SECRET: Optional[Tuple[Optional[str], str]] = (
        "event_api_client_secret_dev",
        Variable.get("event_api_client_secret_dev"),
    )
    OAUTH2_URL = Variable.get("event_api_oauth2_url_dev")
    BIGQUERY_PROJECT = "dwh_project_dev"
    SCHEMA_ID = Variable.get(
        "pos_revenue_schema_id_dev",
        default_var=PROD_SCHEMA_ID_DEFAULT,
    )
    INGEST_BASE = Variable.get(
        "event_api_ingest_base_dev",
        default_var="https://api.example.com/event-ingest/bulk",
    )
else:
    OAUTH_USERNAME = Variable.get("event_api_username")
    OAUTH_PASSWORD = Variable.get("event_api_password")
    CLIENT_ID = Variable.get("event_api_client_id")
    prod_secrets: List[Tuple[Optional[str], str]] = []
    for _name in ("event_api_client_secret",):
        try:
            _val = Variable.get(_name)
        except Exception:
            _val = None
        if _val:
            prod_secrets.append((_name, _val))
    CLIENT_SECRET = prod_secrets[0] if prod_secrets else None
    OAUTH2_URL = Variable.get("event_api_oauth2_url")
    BIGQUERY_PROJECT = "dwh_project"
    SCHEMA_ID = Variable.get(
        "pos_revenue_schema_id",
        default_var=PROD_SCHEMA_ID_DEFAULT,
    )
    INGEST_BASE = Variable.get(
        "event_api_ingest_base",
        default_var="https://api.example.com/event-ingest/bulk",
    )

POS_REVENUE_SCHEMA = """{
  "namespace": "platform",
  "type": "record",
  "name": "platform_pos_revenue",
  "doc": "Platform POS net revenue and annualised revenue potential at wholesale customer grain (country_code, customer_no, home_store_id). Same population and annualised amount as the SAM pos_potential annual_sale contract.",
  "gdpr_info": {
    "table_PII": "no",
    "column_PII": []
  },
  "fields": [
    {"name": "country_code", "type": ["null", "string"], "default": null, "doc": "ISO 3166-1 alpha-2 of the matched wholesale customer"},
    {"name": "customer_no", "type": ["null", "long"], "default": null, "doc": "Wholesale customer number (SAM cust_no)"},
    {"name": "home_store_id", "type": ["null", "long"], "default": null, "doc": "Wholesale home store of that customer"},
    {"name": "wholesale_id", "type": ["null", "string"], "default": null, "doc": "Wholesale id on the POS transactions"},
    {"name": "first_pos_transaction_date", "type": ["null", "string"], "default": null, "doc": "First POS sale day included in the revenue, YYYY-MM-DD"},
    {"name": "last_pos_transaction_date", "type": ["null", "string"], "default": null, "doc": "Last POS sale day included; the 12-month window ends on this date"},
    {"name": "pos_sale_days_last_12_months", "type": ["null", "long"], "default": null, "doc": "Distinct POS sale days in the 12-month window"},
    {"name": "pos_net_revenue_last_12_months", "type": ["null", "double"], "default": null, "doc": "Net POS sales in the 12 months ending on last_pos_transaction_date. Not annualised. Negatives floored to 0"},
    {"name": "annualised_pos_net_revenue", "type": ["null", "double"], "default": null, "doc": "Same value as SAM annual_sale. Equals trailing net unless the licence has been live under a year and is not suspended, then scaled to a full year"}
  ]
}"""


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


class EventIngestAPI:
    """OAuth + chunked POST for the partner event bus.

    Tries client_credentials across configured secrets first, then
    password-grant. 401 on ingest refreshes the token and retries the
    same payload — monthly full loads can outlive a short-lived token.
    """

    def __init__(self, token_url, client_id, client_secret):
        self.token_url = token_url
        self.token = None
        self.client_id = client_id
        self.client_secrets: List[Tuple[Optional[str], str]] = []
        if client_secret:
            if isinstance(client_secret, tuple) and len(client_secret) == 2:
                self.client_secrets = [client_secret]
            elif isinstance(client_secret, (list, tuple)):
                for s in client_secret:
                    if isinstance(s, tuple) and len(s) == 2:
                        self.client_secrets.append(s)
                    elif s:
                        self.client_secrets.append((None, s))
            else:
                self.client_secrets = [(None, client_secret)]
        self.successful_credential = None

    def get_token(self):
        realm = None
        for _var in ("EVENT_API_REALM", "event_api_realm", "event_api_realm_id"):
            try:
                _val = Variable.get(_var)
            except Exception:
                _val = None
            if _val:
                realm = _val
                break

        last_exc = None

        for name, candidate in self.client_secrets:
            if not candidate:
                continue
            headers = {
                "Authorization": "Basic " + _b64(self.client_id + ":" + candidate),
                "Content-Type": "application/x-www-form-urlencoded",
            }
            params_cc = {
                "grant_type": "client_credentials",
                "client_id": self.client_id,
            }
            if realm:
                params_cc["realm_id"] = realm
            try:
                r = requests.post(
                    self.token_url, headers=headers, data=params_cc, timeout=30
                )
                r.raise_for_status()
                resp_json = r.json()
                if "access_token" in resp_json:
                    self.token = resp_json["access_token"]
                    self.successful_credential = name or "unnamed_secret"
                    logging.info(
                        "Acquired token with client_credentials using: %s",
                        self.successful_credential,
                    )
                    return self.token
                logging.error(
                    "client_credentials response missing access_token: %s", resp_json
                )
                last_exc = RuntimeError(
                    "Missing access_token in client_credentials response"
                )
            except Exception as e:
                logging.warning(
                    "client_credentials attempt failed for candidate %s: %s",
                    name or "<unnamed>",
                    e,
                )
                last_exc = e

        params_pw = {
            "grant_type": "password",
            "username": OAUTH_USERNAME,
            "password": OAUTH_PASSWORD,
        }
        if realm:
            params_pw["realm_id"] = realm

        if self.client_secrets:
            for name, candidate in self.client_secrets:
                if not candidate:
                    continue
                headers = {
                    "Authorization": "Basic "
                    + _b64(self.client_id + ":" + candidate),
                    "Content-Type": "application/x-www-form-urlencoded",
                }
                try:
                    r = requests.post(
                        self.token_url, headers=headers, data=params_pw, timeout=30
                    )
                    r.raise_for_status()
                    resp_json = r.json()
                    if "access_token" in resp_json:
                        self.token = resp_json["access_token"]
                        self.successful_credential = (
                            name or "unnamed_secret"
                        ) + "/password"
                        logging.info(
                            "Acquired token with password grant using: %s",
                            self.successful_credential,
                        )
                        return self.token
                    last_exc = RuntimeError(
                        "Missing access_token in password grant response"
                    )
                except Exception as e:
                    logging.warning(
                        "password grant failed for candidate %s: %s",
                        name or "<unnamed>",
                        e,
                    )
                    last_exc = e
        else:
            try:
                r = requests.post(self.token_url, data=params_pw, timeout=30)
                r.raise_for_status()
                resp_json = r.json()
                if "access_token" in resp_json:
                    self.token = resp_json["access_token"]
                    self.successful_credential = "password"
                    logging.info(
                        "Acquired token with password grant (no client secret)"
                    )
                    return self.token
                last_exc = RuntimeError(
                    "Missing access_token in password grant response"
                )
            except Exception as e:
                logging.warning("password grant failed (no client secret): %s", e)
                last_exc = e

        logging.error("All token authentication options completely exhausted.")
        if last_exc:
            raise last_exc
        raise RuntimeError("Failed to obtain access token")

    def endpoint(self, url, data=None, max_retries=10):
        if self.token is None:
            self.get_token()

        headers = {
            "Authorization": "Bearer " + self.token,
            "Content-Type": "application/vnd.example.events.json",
            "accept": "application/vnd.example.events.json",
        }

        for attempt in range(max_retries):
            try:
                r = requests.post(url, headers=headers, data=data, timeout=120)

                if r.status_code == 401:
                    logging.warning(
                        "Received 401 from event ingest, refreshing token and retrying"
                    )
                    self.token = None
                    self.get_token()
                    headers["Authorization"] = "Bearer " + self.token
                    continue

                try:
                    r.raise_for_status()
                except requests.HTTPError:
                    try:
                        body = r.text
                    except Exception:
                        body = "<unreadable body>"
                    logging.error(
                        "Event ingest returned error status %s, body: %s",
                        r.status_code,
                        body,
                    )
                    raise

                try:
                    return r.json()
                except ValueError:
                    logging.info(
                        "Event ingest returned non-JSON response: %s",
                        getattr(r, "text", None),
                    )
                    return r.text

            except (
                requests.exceptions.JSONDecodeError,
                requests.exceptions.RequestException,
            ) as e:
                wait = min(5 * (attempt + 1), 60)
                try:
                    resp_text = (
                        getattr(e.response, "text", None)
                        if hasattr(e, "response")
                        else None
                    )
                except Exception:
                    resp_text = None
                logging.warning(
                    "Event API attempt %d/%d failed for url=%s: %s; response=%s — retrying in %ds",
                    attempt + 1,
                    max_retries,
                    url,
                    e,
                    resp_text,
                    wait,
                )
                logging.exception(e)
                if attempt < max_retries - 1:
                    time.sleep(wait)
                else:
                    raise


def _build_query() -> str:
    """POS revenue potential extract for one country. @country_code is ISO alpha-2."""
    asset = f"`{SOURCE_PROJECT}.product_spot.erp_asset`"
    transactions = (
        f"`{SOURCE_PROJECT}.product_spot.pos_sold_item_transactions_erp`"
    )
    match = f"`{SOURCE_PROJECT}.dwh_refined.match_result_erp_wholesale`"
    return f"""
WITH pos_lifecycle AS (
  SELECT
    establishment_id,
    MIN(onboarding_date) AS onboarding_date,
    ARRAY_AGG(asset_status ORDER BY asset_created_date DESC LIMIT 1)[OFFSET(0)]
      AS latest_asset_status
  FROM {asset}
  WHERE product_code IN (
      'POS_L_Licence',
      'POS_L_Licence_Flex',
      'POS_L_DPL_POS_License'
    )
    AND asset_status IN ('Active', 'Disabled', 'Suspended')
  GROUP BY establishment_id
),

latest_dates AS (
  SELECT
    establishment_id,
    MAX(workday) AS last_pos_date
  FROM {transactions}
  GROUP BY establishment_id
),

pos_sale_days AS (
  SELECT
    pos.establishment_id,
    COUNT(DISTINCT pos.workday) AS pos_sale_days_last_12_months
  FROM {transactions} AS pos
  INNER JOIN latest_dates AS ld
    ON pos.establishment_id = ld.establishment_id
  WHERE pos.workday >= DATE_SUB(ld.last_pos_date, INTERVAL 12 MONTH)
    AND pos.workday <= ld.last_pos_date
  GROUP BY pos.establishment_id
),

pos_potential AS (
  SELECT
    m.country_code,
    m.customer_no,
    m.home_store_id,
    ANY_VALUE(pos.wholesale_id) AS wholesale_id,
    MIN(pos.workday) AS first_pos_transaction_date,
    ld.last_pos_date AS last_pos_transaction_date,
    sale_days.pos_sale_days_last_12_months,
    lc.onboarding_date,
    lc.latest_asset_status,
    GREATEST(0, SUM(pos.sl_net_amount)) AS pos_net_revenue_last_12_months
  FROM {transactions} AS pos
  INNER JOIN latest_dates AS ld
    ON pos.establishment_id = ld.establishment_id
  INNER JOIN pos_sale_days AS sale_days
    ON pos.establishment_id = sale_days.establishment_id
  LEFT JOIN pos_lifecycle AS lc
    ON pos.establishment_id = lc.establishment_id
  LEFT JOIN {match} AS m
    ON pos.establishment_id = m.establishment_id
  WHERE pos.workday >= DATE_SUB(ld.last_pos_date, INTERVAL 12 MONTH)
    AND pos.workday <= ld.last_pos_date
    AND m.country_code = @country_code
  GROUP BY
    pos.establishment_id,
    pos.store_license_id,
    m.country_code,
    m.customer_no,
    m.home_store_id,
    lc.latest_asset_status,
    lc.onboarding_date,
    ld.last_pos_date,
    sale_days.pos_sale_days_last_12_months
  HAVING DATE_DIFF(
    MAX(pos.workday),
    DATE_TRUNC(MIN(pos.workday), WEEK(MONDAY)),
    WEEK(MONDAY)
  ) >= 4
)

SELECT
  CAST(country_code AS STRING) AS country_code,
  CAST(customer_no AS INT64) AS customer_no,
  CAST(home_store_id AS INT64) AS home_store_id,
  CAST(wholesale_id AS STRING) AS wholesale_id,
  FORMAT_DATE('%Y-%m-%d', first_pos_transaction_date) AS first_pos_transaction_date,
  FORMAT_DATE('%Y-%m-%d', last_pos_transaction_date) AS last_pos_transaction_date,
  CAST(pos_sale_days_last_12_months AS INT64) AS pos_sale_days_last_12_months,
  CAST(pos_net_revenue_last_12_months AS FLOAT64) AS pos_net_revenue_last_12_months,
  CAST(
    CASE
      WHEN DATE_DIFF(last_pos_transaction_date, onboarding_date, DAY) BETWEEN 0 AND 364
        AND latest_asset_status != 'Suspended'
      THEN pos_net_revenue_last_12_months
           / (DATE_DIFF(last_pos_transaction_date, onboarding_date, DAY) + 1)
           * 365
      ELSE pos_net_revenue_last_12_months
    END AS FLOAT64
  ) AS annualised_pos_net_revenue
FROM pos_potential
WHERE country_code = @country_code
  AND customer_no IS NOT NULL
  AND home_store_id IS NOT NULL
  AND pos_net_revenue_last_12_months IS NOT NULL
QUALIFY COUNT(*) OVER (
  PARTITION BY country_code, customer_no, home_store_id
) = 1
ORDER BY country_code, customer_no, home_store_id
"""


def _row_to_avro(row) -> dict:
    d = dict(row.items())
    return {
        "country_code": _str(d, "country_code"),
        "customer_no": _long(d, "customer_no"),
        "home_store_id": _long(d, "home_store_id"),
        "wholesale_id": _str(d, "wholesale_id"),
        "first_pos_transaction_date": _str(d, "first_pos_transaction_date"),
        "last_pos_transaction_date": _str(d, "last_pos_transaction_date"),
        "pos_sale_days_last_12_months": _long(d, "pos_sale_days_last_12_months"),
        "pos_net_revenue_last_12_months": _double(d, "pos_net_revenue_last_12_months"),
        "annualised_pos_net_revenue": _double(d, "annualised_pos_net_revenue"),
    }


def _encode_row(row, writer) -> str:
    bw = io.BytesIO()
    encoder = avro.io.BinaryEncoder(bw)
    writer.write(_row_to_avro(row), encoder)
    return base64.b64encode(bw.getvalue()).decode("utf-8")


def _ingestbulk_url(country: str, schema_id: str) -> str:
    return "{}/{}/{}".format(INGEST_BASE.rstrip("/"), country.lower(), schema_id)


def send_pos_revenue_data(country="es"):
    """Full-load platform_pos_revenue for one country to partner ingestbulk."""
    country_code = country.upper()
    if country.lower() not in COUNTRY_ISO_CODES:
        raise ValueError(
            f"Unsupported country {country!r}. Expected one of {COUNTRY_ISO_CODES}."
        )

    api = EventIngestAPI(OAUTH2_URL, CLIENT_ID, CLIENT_SECRET)
    api.get_token()
    logging.info(
        "Event API token acquired using credential: %s",
        getattr(api, "successful_credential", None),
    )

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("country_code", "STRING", country_code),
        ]
    )
    label = "platform_pos_revenue"
    print(
        f"{datetime.now()} - [{country_code}] {label} querying BQ {SOURCE_PROJECT}..."
    )
    client = bigquery.Client(project=BIGQUERY_PROJECT)
    results = client.query(_build_query(), job_config=job_config).result()
    total_rows = results.total_rows
    logging.info(
        "%s - [%s] %s query rows: %s",
        datetime.now(),
        country_code,
        label,
        total_rows,
    )

    base_url = _ingestbulk_url(country, SCHEMA_ID)
    logging.info(
        "%s - [%s] %s ingest URL: %s",
        datetime.now(),
        country_code,
        label,
        base_url,
    )

    if not total_rows:
        print(f"{datetime.now()} - [{country_code}] {label} DONE. Total rows: 0")
        return

    schema_parsed = avro.schema.parse(POS_REVENUE_SCHEMA)
    writer = avro.io.DatumWriter(schema_parsed)

    row_count = 0
    encoded = (_encode_row(row, writer) for row in results)
    for chunk in batched(encoded, CHUNK_SIZE):
        payload = json.dumps({"records": [{"value": rec} for rec in chunk]})
        try:
            resp = api.endpoint(url=base_url, data=payload)
            logging.info("Event ingest response: %s", resp)
        except Exception:
            logging.exception(
                "Failed to send chunk for country %s label %s. Payload size=%d",
                country_code,
                label,
                len(chunk),
            )
            raise
        row_count += len(chunk)
        print(f"{datetime.now()} - [{country_code}] {label} rows {row_count} — {resp}")

    print(f"{datetime.now()} - [{country_code}] {label} DONE. Total rows: {row_count}")


if __name__ == "__main__":
    # Smoke: schema parse + SQL build. No BQ / network.
    avro.schema.parse(POS_REVENUE_SCHEMA)
    sql = _build_query()
    assert "@country_code" in sql
    assert "annualised_pos_net_revenue" in sql
    assert "QUALIFY COUNT(*) OVER" in sql
    print("ok: Avro schema + SQL smoke passed")
