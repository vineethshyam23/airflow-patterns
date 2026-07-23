"""
Avro bulk ingest for Odoo / CRM lead lifecycle, asset lifecycle, and
voucher-code deltas.

Three send functions share one OAuth client pattern: BigQuery → Avro
encode → chunk 500 → POST /ingestbulk/{country}/{schema_id}.

Credentials and schema ids come from Airflow Variables only.

Source (read-only):
  dags/horeca_digital/dana_odoo_assets_leads_lifecycle_export.py

Sanitized fixes vs production module:
  - Avro schema parsed once per send (source parsed every row)
  - HTTP errors raise instead of only logging the body
  - Schema ids / ingest base externalized to Variables
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
    LEAD_SCHEMA_ID = Variable.get(
        "odoo_lead_lifecycle_event_schema_id_dev", default_var="LEAD_SCHEMA_DEV"
    )
    ASSET_SCHEMA_ID = Variable.get(
        "odoo_asset_lifecycle_event_schema_id_dev", default_var="ASSET_SCHEMA_DEV"
    )
    VOUCHER_SCHEMA_ID = Variable.get(
        "odoo_voucher_code_event_schema_id_dev", default_var="VOUCHER_SCHEMA_DEV"
    )
    LEAD_SCHEMA_NAME = "crm_lead_lifecycle_dev"
    ASSET_SCHEMA_NAME = "crm_asset_lifecycle_dev"
    VOUCHER_SCHEMA_NAME = "odoo_voucher_code_dev"
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
    LEAD_SCHEMA_ID = Variable.get(
        "odoo_lead_lifecycle_event_schema_id", default_var="LEAD_SCHEMA_PROD"
    )
    ASSET_SCHEMA_ID = Variable.get(
        "odoo_asset_lifecycle_event_schema_id", default_var="ASSET_SCHEMA_PROD"
    )
    VOUCHER_SCHEMA_ID = Variable.get(
        "odoo_voucher_code_event_schema_id", default_var="VOUCHER_SCHEMA_PROD"
    )
    LEAD_SCHEMA_NAME = "crm_lead_lifecycle"
    ASSET_SCHEMA_NAME = "crm_asset_lifecycle"
    VOUCHER_SCHEMA_NAME = "odoo_voucher_code"
    INGEST_BASE = Variable.get(
        "event_api_ingest_base",
        default_var="https://api.example.com/event-ingest/bulk",
    )


def chunks(items: List, size: int) -> Iterator[List]:
    for i in range(0, len(items), size):
        yield items[i : i + size]


def _b64(s: str) -> str:
    return base64.b64encode(s.encode()).decode()


def _avro_date(value):
    """BigQuery DATE → Avro logical date (int days since 1970-01-01)."""
    if value is None:
        return None
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


def _post_chunks(client: EventApiClient, country: str, schema_id: str, encoded: List[str]):
    base_url = f"{INGEST_BASE.rstrip('/')}/{country}/{schema_id}"
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


# --- Avro contracts ---------------------------------------------------------
# Field sets mirror the refined tables the partner consumer registered.
# Note: production schemas marked table_PII=no while lead/asset payloads
# still carry email / phone / name. Treat classification as a governance
# follow-up, not as "this is anonymized."

LEAD_AVRO_SCHEMA = json.dumps(
    {
        "namespace": "company",
        "type": "record",
        "name": LEAD_SCHEMA_NAME,
        "doc": "CRM lead lifecycle delta for partner market analytics",
        "fields": [
            {"name": "lead_id", "type": "string"},
            {"name": "converted_account_id", "type": "string"},
            {"name": "lead_referrer", "type": "string"},
            {"name": "store", "type": "string"},
            {"name": "metro_id", "type": "string"},
            {"name": "customer_id_sam", "type": "string"},
            {"name": "establishment_name", "type": "string"},
            {"name": "lead_full_name", "type": "string"},
            {"name": "product_name", "type": "string"},
            {"name": "lead_creation_date", "type": "string"},
            {"name": "closing_date", "type": "string"},
            {"name": "lead_source", "type": "string"},
            {"name": "status", "type": "string"},
            {"name": "reason_lost", "type": "string"},
            {"name": "reason_lost2", "type": "string"},
            {"name": "converted_contact_id", "type": "string"},
            {"name": "lead_owner_name", "type": "string"},
            {"name": "channel_v2", "type": "string"},
            {"name": "asset_creation_date", "type": "string"},
            {"name": "activated_by", "type": "string"},
            {"name": "lead_email", "type": "string"},
            {"name": "lead_street", "type": "string"},
            {"name": "lead_city", "type": "string"},
            {"name": "lead_postal_code", "type": "string"},
            {"name": "lead_country_code", "type": "string"},
            {"name": "_ldts", "type": {"type": "int", "logicalType": "date"}},
            {"name": "ecom_traffic_source", "type": ["null", "string"], "default": None},
            {"name": "asset_id", "type": ["null", "string"], "default": None},
        ],
    }
)

ASSET_AVRO_SCHEMA = json.dumps(
    {
        "namespace": "company",
        "type": "record",
        "name": ASSET_SCHEMA_NAME,
        "doc": "CRM / Odoo asset lifecycle delta for partner market analytics",
        "fields": [
            {"name": "account_id", "type": ["null", "string"], "default": None},
            {"name": "mcc_metro_id", "type": ["null", "string"], "default": None},
            {"name": "mcc_home_store_id", "type": ["null", "string"], "default": None},
            {"name": "establishment_id", "type": ["null", "string"], "default": None},
            {
                "name": "sfdc_internal_establishment_id",
                "type": ["null", "string"],
                "default": None,
            },
            {"name": "country_code", "type": ["null", "string"], "default": None},
            {"name": "full_vat", "type": ["null", "string"], "default": None},
            {"name": "vat_verified", "type": ["null", "boolean"], "default": None},
            {"name": "company_name", "type": ["null", "string"], "default": None},
            {"name": "establishment_name", "type": ["null", "string"], "default": None},
            {"name": "street", "type": ["null", "string"], "default": None},
            {"name": "postalcode", "type": ["null", "string"], "default": None},
            {"name": "city", "type": ["null", "string"], "default": None},
            {
                "name": "establishment_address",
                "type": ["null", "string"],
                "default": None,
            },
            {"name": "first_name", "type": ["null", "string"], "default": None},
            {"name": "last_name", "type": ["null", "string"], "default": None},
            {"name": "mobilephone", "type": ["null", "string"], "default": None},
            {"name": "email", "type": ["null", "string"], "default": None},
            {
                "name": "manager_information",
                "type": ["null", "string"],
                "default": None,
            },
            {
                "name": "dish_terms_conditions",
                "type": ["null", "boolean"],
                "default": None,
            },
            {"name": "asset_UID", "type": ["null", "string"], "default": None},
            {"name": "subscription_id", "type": ["null", "string"], "default": None},
            {"name": "asset_channel_v2", "type": ["null", "string"], "default": None},
            {"name": "asset_status", "type": ["null", "string"], "default": None},
            {"name": "asset_name", "type": ["null", "string"], "default": None},
            {
                "name": "asset_creation_date",
                "type": ["null", "string"],
                "default": None,
            },
            {
                "name": "asset_disabled_date",
                "type": ["null", "string"],
                "default": None,
            },
            {
                "name": "reason_of_cancellation",
                "type": ["null", "string"],
                "default": None,
            },
            {
                "name": "text_reason_of_cancellation",
                "type": ["null", "string"],
                "default": None,
            },
            {"name": "asset_onboarded", "type": ["null", "boolean"], "default": None},
            {
                "name": "asset_onboarding_date",
                "type": ["null", "string"],
                "default": None,
            },
            {"name": "commitment_period", "type": ["null", "int"], "default": None},
            {"name": "asset_referrer", "type": ["null", "string"], "default": None},
            {"name": "price", "type": ["null", "float"], "default": None},
            {"name": "voucher_code", "type": ["null", "string"], "default": None},
            {
                "name": "voucher_reduction_grant_month",
                "type": ["null", "int"],
                "default": None,
            },
            {"name": "onetime_percentage", "type": ["null", "float"], "default": None},
            {
                "name": "recurring_percentage",
                "type": ["null", "float"],
                "default": None,
            },
            {
                "name": "onetime_original_net_price",
                "type": ["null", "float"],
                "default": None,
            },
            {
                "name": "recurring_original_net_price",
                "type": ["null", "float"],
                "default": None,
            },
            {"name": "_ldts", "type": {"type": "int", "logicalType": "date"}},
            {"name": "asset_activated_by", "type": ["null", "string"], "default": None},
            {"name": "asset_migrated", "type": ["null", "string"], "default": None},
            {
                "name": "asset_onboarded_sfdc",
                "type": ["null", "string"],
                "default": None,
            },
            {
                "name": "asset_onboarding_date_sfdc",
                "type": ["null", "string"],
                "default": None,
            },
            {"name": "product_code", "type": ["null", "string"], "default": None},
            {
                "name": "country_specific_code",
                "type": ["null", "string"],
                "default": None,
            },
            {"name": "asset_install_date", "type": ["null", "string"], "default": None},
            {"name": "is_subscription", "type": ["null", "boolean"], "default": None},
            {"name": "assignees", "type": ["null", "string"], "default": None},
            {"name": "quantity", "type": ["null", "int"], "default": None},
            {"name": "lead_id", "type": ["null", "int"], "default": None},
            {"name": "created_from_lead", "type": ["null", "string"], "default": None},
            {
                "name": "odoo_OnboardingDate",
                "type": ["null", "string"],
                "default": None,
            },
            {"name": "odoo_Onboarding_flag", "type": ["null", "int"], "default": None},
            {"name": "odoo_metro_id", "type": ["null", "string"], "default": None},
            {"name": "odoo_store_id", "type": ["null", "string"], "default": None},
            {
                "name": "establishment_active",
                "type": ["null", "boolean"],
                "default": None,
            },
        ],
    }
)

VOUCHER_AVRO_SCHEMA = json.dumps(
    {
        "namespace": "company",
        "type": "record",
        "name": VOUCHER_SCHEMA_NAME,
        "doc": "Odoo voucher / discount codes for partner market analytics",
        "fields": [
            {"name": "asset_id", "type": ["null", "string"], "default": None},
            {"name": "sale_order_id", "type": ["null", "string"], "default": None},
            {"name": "partner_id", "type": ["null", "long"], "default": None},
            {"name": "establishment_id", "type": ["null", "string"], "default": None},
            {"name": "order_id", "type": ["null", "string"], "default": None},
            {"name": "subscription_id", "type": ["null", "string"], "default": None},
            {"name": "product_code", "type": ["null", "string"], "default": None},
            {"name": "discount_code", "type": ["null", "string"], "default": None},
            {"name": "discount_desc", "type": ["null", "string"], "default": None},
            {"name": "price", "type": ["null", "double"], "default": None},
            {"name": "recurring_invoice", "type": ["null", "boolean"], "default": None},
            {"name": "is_subscription", "type": ["null", "boolean"], "default": None},
            {
                "name": "asset_installation_date",
                "type": ["null", "string"],
                "default": None,
            },
            {"name": "asset_created_date", "type": ["null", "string"], "default": None},
            {"name": "country_code", "type": ["null", "string"], "default": None},
        ],
    }
)


def send_lead_lifecycle_data(country: str, query: str) -> None:
    """Run the leads SCD delta query, Avro-encode, POST in chunks of 500."""
    client = EventApiClient(OAUTH2_URL, CLIENT_ID, CLIENT_SECRET)
    schema = avro.schema.parse(LEAD_AVRO_SCHEMA)

    logging.info("%s - Executing lead lifecycle query...", datetime.now())
    bq = bigquery.Client(project=BIGQUERY_PROJECT)
    results = bq.query(query).result()

    encoded: List[str] = []
    for row in results:
        payload = {
            "lead_id": row["lead_id"],
            "converted_account_id": row["converted_account_id"],
            "lead_referrer": row["lead_referrer"],
            "store": row["store"],
            "metro_id": row["metro_id"],
            "customer_id_sam": row["customer_id_sam"],
            "establishment_name": row["establishment_name"],
            "lead_full_name": row["lead_full_name"],
            "product_name": row["product_name"],
            "lead_creation_date": row["lead_creation_date"],
            "closing_date": row["closing_date"],
            "lead_source": row["lead_source"],
            "status": row["status"],
            "reason_lost": row["reason_lost"],
            "reason_lost2": row["reason_lost2"],
            "converted_contact_id": row["converted_contact_id"],
            "lead_owner_name": row["lead_owner_name"],
            "channel_v2": row["channel_v2"],
            "asset_creation_date": row["asset_creation_date"],
            "activated_by": row["activated_by"],
            "lead_email": row["lead_email"],
            "lead_street": row["lead_street"],
            "lead_city": row["lead_city"],
            "lead_postal_code": row["lead_postal_code"],
            "lead_country_code": row["lead_country_code"],
            "_ldts": _avro_date(row["_ldts"]),
            "ecom_traffic_source": row["ecom_traffic_source"],
            "asset_id": row["asset_id"],
        }
        encoded.append(_encode_bytes(payload, schema))

    logging.info(
        "%s - %s lead rows to send for %s",
        datetime.now(),
        len(encoded),
        country.upper(),
    )
    _post_chunks(client, country, LEAD_SCHEMA_ID, encoded)
    logging.info(
        "%s - Lead lifecycle ingest completed for %s", datetime.now(), country.upper()
    )


def send_asset_lifecycle_data(country: str, query: str) -> None:
    """Run the assets SCD delta query, Avro-encode, POST in chunks of 500."""
    client = EventApiClient(OAUTH2_URL, CLIENT_ID, CLIENT_SECRET)
    schema = avro.schema.parse(ASSET_AVRO_SCHEMA)

    logging.info("%s - Executing asset lifecycle query...", datetime.now())
    bq = bigquery.Client(project=BIGQUERY_PROJECT)
    results = bq.query(query).result()

    encoded: List[str] = []
    for row in results:
        payload = {
            "account_id": row["account_id"],
            "mcc_metro_id": row["mcc_metro_id"],
            "mcc_home_store_id": row["mcc_home_store_id"],
            "establishment_id": row["establishment_id"],
            "sfdc_internal_establishment_id": row["sfdc_internal_establishment_id"],
            "country_code": row["country_code"],
            "full_vat": row["full_vat"],
            "vat_verified": row["vat_verified"],
            "company_name": row["company_name"],
            "establishment_name": row["establishment_name"],
            "street": row["street"],
            "postalcode": row["postalcode"],
            "city": row["city"],
            "establishment_address": row["establishment_address"],
            "first_name": row["first_name"],
            "last_name": row["last_name"],
            "mobilephone": row["mobilephone"],
            "email": row["email"],
            "manager_information": row["manager_information"],
            "dish_terms_conditions": row["dish_terms_conditions"],
            "asset_UID": row["asset_UID"],
            "subscription_id": row["subscription_id"],
            "asset_channel_v2": row["asset_channel_v2"],
            "asset_status": row["asset_status"],
            "asset_name": row["asset_name"],
            "asset_creation_date": row["asset_creation_date"],
            "asset_disabled_date": row["asset_disabled_date"],
            "reason_of_cancellation": row["reason_of_cancellation"],
            "text_reason_of_cancellation": row["text_reason_of_cancellation"],
            "asset_onboarded": row["asset_onboarded"],
            "asset_onboarding_date": row["asset_onboarding_date"],
            "commitment_period": row["commitment_period"],
            "asset_referrer": row["asset_referrer"],
            "price": row["price"],
            "voucher_code": row["voucher_code"],
            "voucher_reduction_grant_month": row["voucher_reduction_grant_month"],
            "onetime_percentage": row["onetime_percentage"],
            "recurring_percentage": row["recurring_percentage"],
            "onetime_original_net_price": row["onetime_original_net_price"],
            "recurring_original_net_price": row["recurring_original_net_price"],
            "_ldts": _avro_date(row["_ldts"]),
            "asset_activated_by": row["asset_activated_by"],
            "asset_migrated": row["asset_migrated"],
            "asset_onboarded_sfdc": row["asset_onboarded_sfdc"],
            "asset_onboarding_date_sfdc": row["asset_onboarding_date_sfdc"],
            "product_code": row["product_code"],
            "country_specific_code": row["country_specific_code"],
            "asset_install_date": row["asset_install_date"],
            "is_subscription": row["is_subscription"],
            "assignees": row["assignees"],
            "quantity": row["quantity"],
            "lead_id": row["lead_id"],
            "created_from_lead": row["created_from_lead"],
            "odoo_OnboardingDate": row["odoo_OnboardingDate"],
            # BQ column odoo_Onboarding_status → Avro field odoo_Onboarding_flag
            "odoo_Onboarding_flag": row["odoo_Onboarding_status"],
            "odoo_metro_id": row["odoo_metro_id"],
            "odoo_store_id": row["odoo_store_id"],
            "establishment_active": row["establishment_active"],
        }
        encoded.append(_encode_bytes(payload, schema))

    logging.info(
        "%s - %s asset rows to send for %s",
        datetime.now(),
        len(encoded),
        country.upper(),
    )
    _post_chunks(client, country, ASSET_SCHEMA_ID, encoded)
    logging.info(
        "%s - Asset lifecycle ingest completed for %s", datetime.now(), country.upper()
    )


def send_voucher_code_data(country: str, query: str) -> None:
    """Run the voucher created-date window query, Avro-encode, POST chunks."""
    client = EventApiClient(OAUTH2_URL, CLIENT_ID, CLIENT_SECRET)
    schema = avro.schema.parse(VOUCHER_AVRO_SCHEMA)

    logging.info("%s - Executing voucher code query...", datetime.now())
    bq = bigquery.Client(project=BIGQUERY_PROJECT)
    results = bq.query(query).result()

    encoded: List[str] = []
    for row in results:
        payload = {
            "asset_id": row["asset_id"],
            "sale_order_id": row["sale_order_id"],
            "partner_id": row["partner_id"],
            "establishment_id": row["establishment_id"],
            "order_id": row["order_id"],
            "subscription_id": row["subscription_id"],
            "product_code": row["product_code"],
            "discount_code": row["discount_code"],
            "discount_desc": row["discount_desc"],
            "price": row["price"],
            "recurring_invoice": row["recurring_invoice"],
            "is_subscription": row["is_subscription"],
            "asset_installation_date": row["asset_installation_date"],
            "asset_created_date": row["asset_created_date"],
            "country_code": row["country_code"],
        }
        encoded.append(_encode_bytes(payload, schema))

    logging.info(
        "%s - %s voucher rows to send for %s",
        datetime.now(),
        len(encoded),
        country.upper(),
    )
    _post_chunks(client, country, VOUCHER_SCHEMA_ID, encoded)
    logging.info(
        "%s - Voucher code ingest completed for %s", datetime.now(), country.upper()
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    raise SystemExit(
        "Call send_*_data(country, query) from Airflow or a notebook — "
        "not as a standalone script without BQ + OAuth."
    )
