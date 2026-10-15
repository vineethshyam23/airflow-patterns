"""Wholesale NL enrichment → partner CRM reverse export helpers.

BigQuery discovery tables (prospects, matched, dedupe pairs) → chunked
OAuth2 POSTs into the partner MCC HubSpot-facing endpoints. Sibling of
the inbound dual-source land (pattern 69); this module is outbound only.

Production quirks kept visible:
- Full-table export (no incremental watermark)
- Snapshot table names parameterized via Airflow Variable
- try/except around POSTs re-raises so Composer fails the task
- Prospects and matched share one schema; dedupe posts id pairs only

Source (read-only):
  dags/horeca_digital/makro_customers_api.py (export_* helpers)
"""

from __future__ import annotations

import base64
import json
import logging
import os
import random
from decimal import Decimal
from typing import Any, Callable, Iterator

import requests
from airflow.models import Variable
from google.cloud import bigquery

logger = logging.getLogger(__name__)

environment = "env"
env = os.environ.get(environment, Variable.get(environment, default_var="DEV"))

if env == "DEV":
    USERNAME = Variable.get("wholesale_nl_mcc_user", default_var="")
    PASSWORD = Variable.get("wholesale_nl_mcc_password", default_var="")
    CLIENT_ID = Variable.get("mcc_oauth_client_id_dev", default_var="")
    CLIENT_SECRET = Variable.get("mcc_oauth_client_secret_dev", default_var="")
    OAUTH2_URL = Variable.get(
        "mcc_oauth2_url_sbx", default_var="https://mcc.example.com/oauth/token"
    )
    project_name = Variable.get("dwh_gcp_project_dev", default_var="dwh_project_dev")
else:
    USERNAME = Variable.get("wholesale_nl_mcc_user", default_var="")
    PASSWORD = Variable.get("wholesale_nl_mcc_password", default_var="")
    CLIENT_ID = Variable.get("mcc_oauth_client_id", default_var="")
    CLIENT_SECRET = Variable.get("mcc_oauth_client_secret", default_var="")
    OAUTH2_URL = Variable.get(
        "mcc_oauth2_url", default_var="https://mcc.example.com/oauth/token"
    )
    project_name = Variable.get("dwh_gcp_project", default_var="dwh_project")

BASE_URL_EXPORT_PROSPECT = Variable.get(
    "wholesale_nl_hubspot_export_prospect_url",
    default_var="https://mcc.example.com/api/hubspot/export/prospects",
)
BASE_URL_EXPORT_MATCHED = Variable.get(
    "wholesale_nl_hubspot_export_matched_url",
    default_var="https://mcc.example.com/api/hubspot/export/matched",
)
BASE_URL_POST_MERGEREQUESTS = Variable.get(
    "wholesale_nl_hubspot_post_merge_url",
    default_var="https://mcc.example.com/api/hubspot/merge-requests",
)

# Enrichment snapshot suffix — production hardcodes a date; Variable avoids
# a deploy every time discovery refreshes.
ENRICHMENT_SNAPSHOT = Variable.get(
    "wholesale_nl_enrichment_snapshot", default_var="20250922"
)
CHUNK_SIZE = int(Variable.get("wholesale_nl_hubspot_chunk_size", default_var="5000"))
DISCOVERY_DATASET = Variable.get("discovery_dataset", default_var="dwh_discovery")


def base64_encode_string(s: str) -> str:
    return base64.b64encode(s.encode()).decode()


def chunks(items: list, n: int) -> Iterator[list]:
    for i in range(0, len(items), n):
        yield items[i : i + n]


class DecimalEncoder(json.JSONEncoder):
    def default(self, obj: Any) -> Any:
        if isinstance(obj, Decimal):
            return str(obj)
        return super().default(obj)


class callAPI:
    """Password-grant OAuth2 client with 401 re-auth on POST."""

    def __init__(self, token_url, client_id, client_secret):
        self.token_url = token_url
        self.token = None
        self.client_id = client_id
        self.client_secret = client_secret

    def get_token(self):
        headers = {
            "Authorization": "Basic "
            + base64_encode_string(self.client_id + ":" + self.client_secret),
            "Content-Type": "application/x-www-form-urlencoded",
        }
        params = {
            "grant_type": "password",
            "username": USERNAME,
            "password": PASSWORD,
        }
        r = requests.post(self.token_url, headers=headers, params=params, timeout=60)
        r.raise_for_status()
        self.token = r.json()["access_token"]
        return self.token

    def endpoint_post(self, url, data=None):
        if self.token is None:
            self.get_token()
        headers = {
            "Authorization": "Bearer " + self.token,
            "Content-Type": "application/json",
        }
        r = requests.post(url, headers=headers, data=data, timeout=120)
        if r.status_code == 401:
            self.token = None
            return self.endpoint_post(url, data)
        r.raise_for_status()
        try:
            return r.json()
        except json.JSONDecodeError as e:
            raise RuntimeError(
                f"Invalid JSON from partner API (status={r.status_code}): {e}"
            ) from e


# Shared enrichment SELECT — prospects and matched use the same payload shape.
_ENRICHMENT_SELECT = """
        ifnull(id, '') as id,
        ifnull(wholesale_cust_key, '') as wholesale_cust_key,
        ifnull(cdm_id, '') as cdm_id,
        ifnull(chd_id, '') as chd_id,
        ifnull(menu_eng_id, '') as menu_eng_id,
        ifnull(pos_vendor_id, '') as pos_vendor_id,
        ifnull(DS_id, '') as DS_id,
        ifnull(status, '') as status,
        ifnull(account_name, '') as account_name,
        ifnull(address, '') as address,
        ifnull(house_number, '') as house_number,
        ifnull(zipcode, '') as zipcode,
        ifnull(city, '') as city,
        ifnull(region_1, '') as region_1,
        ifnull(country, '') as country,
        ifnull(phone, '') as phone,
        ifnull(website, '') as website,
        ifnull(legal_id, '') as legal_id,
        ifnull(branch_family_desc, '') as branch_family_desc,
        ifnull(branch_family_id, 0) as branch_family_id,
        ifnull(revenue_last_3_months, '') as revenue_last_3_months,
        ifnull(number_of_tables, '') as number_of_tables,
        ifnull(number_of_menu_items, 0) as number_of_menu_items,
        ifnull(price_range, '') as price_range,
        ifnull(mcc_distance_air_km, 0) as mcc_distance_air_km,
        ifnull(mcc_distance_km, 0) as mcc_distance_km,
        ifnull(mcc_distance_minutes, 0) as mcc_distance_minutes,
        ifnull(competitor_density, 0) as competitor_density,
        ifnull(has_online_reservation, FALSE) as has_online_reservation,
        ifnull(has_delivery_takeaway, FALSE) as has_delivery_takeaway,
        ifnull(poi_density, 0) as poi_density,
        ifnull(cuisine_competitor_density, 0) as cuisine_competitor_density,
        ifnull(discounter_density, 0) as discounter_density,
        ifnull(supermarket_density, 0) as supermarket_density,
        ifnull(cashcarry_density, 0) as cashcarry_density,
        ifnull(establishment_type, '') as establishment_type,
        ifnull(cuisine_type, '') as cuisine_type,
        ifnull(digitalisation_index, 0) as digitalisation_index,
        ifnull(open_hours, '') as open_hours,
        ifnull(popularity_rate, 0.0) as popularity_rate,
        ifnull(chd_confidence_level, '') as chd_confidence_level,
        ifnull(simplified_market_segment__gfc2_, '') as simplified_market_segment__gfc2_,
        ifnull(detailed_market_segment__gfc4_, '') as detailed_market_segment__gfc4_,
        ifnull(detailed_menu,'') as detailed_menu,
        ifnull(years_in_business, '') as years_in_business,
        ifnull(number_of_employees, '') as number_of_employees,
        ifnull(chain_id, '') as chain_id,
        ifnull(chain_name, '') as chain_name,
        ifnull(annual_sales_range, '') as annual_sales_range,
        ifnull(average_check, '') as average_check,
        ifnull(number_of_meals, '') as number_of_meals,
        ifnull(number_of_rooms, '') as number_of_rooms,
        ifnull(tags_places, '') as tags_places,
        ifnull(channel, '') as channel,
        ifnull(segment_cd, '') as segment_cd,
        ifnull(entity_type, '') as entity_type,
        ifnull(operator_status, '') as operator_status,
        ifnull(chain_size_range_cd, '') as chain_size_range_cd,
        ifnull(chain_size_range_desc, '') as chain_size_range_desc,
        ifnull(cuisine_cd, '') as cuisine_cd,
        ifnull(cuisine_primary_desc, '') as cuisine_primary_desc,
        ifnull(tags_food, '') as tags_food,
        ifnull(offers_dine_in, FALSE) as offers_dine_in,
        ifnull(offers_alcohol, '') as offers_alcohol,
        ifnull(zip_community_type, '') as zip_community_type,
        ifnull(purchasing_power_person, 0.0) as purchasing_power_person,
        ifnull(population_density, 0.0) as population_density,
        ifnull(fbo_potential, 0.0) as fbo_potential
"""


def _enrichment_table(kind: str) -> str:
    return (
        f"{project_name}.{DISCOVERY_DATASET}."
        f"data_enrichment_WholesaleNL_HoReCa_{kind}_{ENRICHMENT_SNAPSHOT}"
    )


def export_query_prospects() -> str:
    return f"select {_ENRICHMENT_SELECT} from {_enrichment_table('prospect')}"


def export_query_matched() -> str:
    return f"select {_ENRICHMENT_SELECT} from {_enrichment_table('matched')}"


def export_query_deduplication() -> str:
    table = (
        f"`{project_name}.{DISCOVERY_DATASET}."
        f"data_enrichment_WholesaleNL_deduplication_{ENRICHMENT_SNAPSHOT}`"
    )
    return f"""
    select ifnull(CAST(winning_id as STRING), '') as winning_id,
           ifnull(CAST(losing_id as STRING), '') as losing_id
    from {table}
    """


def _row_to_enrichment_payload(row) -> dict:
    payload = {
        "id": row["id"],
        "wholesale_cust_key": row["wholesale_cust_key"],
        "cdm_id": row["cdm_id"],
        "chd_id": row["chd_id"],
        "menu_eng_id": row["menu_eng_id"],
        "pos_vendor_id": row["pos_vendor_id"],
        "ds_id": row["DS_id"],
        "status": row["status"],
        "account_name": row["account_name"],
        "address": row["address"],
        "house_number": row["house_number"],
        "zipcode": row["zipcode"],
        "city": row["city"],
        "region_1": row["region_1"],
        "country": row["country"],
        "phone": row["phone"],
        "website": row["website"],
        "legal_id": row["legal_id"],
        "branch_family_desc": row["branch_family_desc"],
        "branch_family_id": row["branch_family_id"],
        "revenue_last_3_months": row["revenue_last_3_months"],
        "number_of_tables": row["number_of_tables"],
        "number_of_menu_items": row["number_of_menu_items"],
        "price_range": row["price_range"],
        "mcc_distance_air_km": row["mcc_distance_air_km"],
        "mcc_distance_km": row["mcc_distance_km"],
        "mcc_distance_minutes": row["mcc_distance_minutes"],
        "competitor_density": row["competitor_density"],
        "has_online_reservation": row["has_online_reservation"],
        "has_delivery_takeaway": row["has_delivery_takeaway"],
        "poi_density": row["poi_density"],
        "cuisine_competitor_density": row["cuisine_competitor_density"],
        "discounter_density": row["discounter_density"],
        "supermarket_density": row["supermarket_density"],
        "cashcarry_density": row["cashcarry_density"],
        "establishment_type": row["establishment_type"],
        "cuisine_type": row["cuisine_type"],
        "digitalisation_index": row["digitalisation_index"],
        "open_hours": row["open_hours"],
        "popularity_rate": row["popularity_rate"],
        "chd_confidence_level": row["chd_confidence_level"],
        "simplified_market_segment__gfc2_": row["simplified_market_segment__gfc2_"],
        "detailed_market_segment__gfc4_": row["detailed_market_segment__gfc4_"],
        "detailed_menu": row["detailed_menu"],
        "years_in_business": row["years_in_business"],
        "number_of_employees": row["number_of_employees"],
        "chain_id": row["chain_id"],
        "chain_name": row["chain_name"],
        "annual_sales_range": row["annual_sales_range"],
        "average_check": row["average_check"],
        "number_of_meals": row["number_of_meals"],
        "number_of_rooms": row["number_of_rooms"],
        "tags_places": row["tags_places"],
        "channel": row["channel"],
        "segment_cd": row["segment_cd"],
        "entity_type": row["entity_type"],
        "operator_status": row["operator_status"],
        "chain_size_range_cd": row["chain_size_range_cd"],
        "chain_size_range_desc": row["chain_size_range_desc"],
        "cuisine_cd": row["cuisine_cd"],
        "cuisine_primary_desc": row["cuisine_primary_desc"],
        "tags_food": row["tags_food"],
        "offers_dine_in": row["offers_dine_in"],
        "offers_alcohol": row["offers_alcohol"],
        "zip_community_type": row["zip_community_type"],
        "purchasing_power_person": row["purchasing_power_person"],
        "population_density": row["population_density"],
        "fbo_potential": row["fbo_potential"],
    }
    return {k: v for k, v in payload.items() if v is not None}


def _post_chunks(url: str, records: list[dict], label: str) -> int:
    # Production used random.randint(1**12, 10**12) — 1**12 == 1.
    session_id = random.randint(10**11, 10**12 - 1)
    chunked = list(chunks(records, CHUNK_SIZE))
    logger.info("%s — records=%s chunks=%s session_id=%s", label, len(records), len(chunked), session_id)
    logger.info("%s — POST url=%s", label, url)

    mcc = callAPI(OAUTH2_URL, CLIENT_ID, CLIENT_SECRET)
    posted = 0
    try:
        for chunk in chunked:
            posted += len(chunk)
            body = {"sessionid": session_id, "records": chunk}
            response = mcc.endpoint_post(
                url=url, data=json.dumps(body, cls=DecimalEncoder) + "\n"
            )
            logger.info("%s — chunk ok size=%s response=%s", label, len(chunk), response)
    except Exception:
        logger.exception("%s — export failed after posting ~%s rows", label, posted)
        raise
    return posted


def _export_enrichment(
    query_fn: Callable[[], str],
    row_fn: Callable[[Any], dict],
    url: str,
    label: str,
) -> int:
    client = bigquery.Client(project=project_name)
    results = client.query(query_fn()).result()
    records = [row_fn(row) for row in results]
    return _post_chunks(url, records, label)


def export_data_prospects() -> int:
    return _export_enrichment(
        export_query_prospects,
        _row_to_enrichment_payload,
        BASE_URL_EXPORT_PROSPECT,
        "prospects",
    )


def export_data_matched() -> int:
    return _export_enrichment(
        export_query_matched,
        _row_to_enrichment_payload,
        BASE_URL_EXPORT_MATCHED,
        "matched",
    )


def export_data_deduplication() -> int:
    def row_fn(row) -> dict:
        return {
            k: v
            for k, v in {
                "id_winning": row["winning_id"],
                "id_losing": row["losing_id"],
            }.items()
            if v is not None
        }

    return _export_enrichment(
        export_query_deduplication,
        row_fn,
        BASE_URL_POST_MERGEREQUESTS,
        "deduplication",
    )
