"""Enrich mailing_ids with Maileon name + tags (per-id REST lookups).

After the eight report staging tables land and the first dbt job builds
int_maileon_* tables, we UNION distinct mailing_ids and call:
  GET /mailings/{id}/name
  GET /mailings/{id}/settings/tags

Writes NDJSON under Composer data/ for a truncate-load into staging.
Expect rate limits — 429/500 use exponential backoff (30s base, ×2).

Source (read-only): dags/horeca_digital/get_maileon_names.py
"""

from __future__ import annotations

import json
import logging
import os
import time
from typing import Any, Dict, List, Optional
from urllib.parse import quote

import requests
from google.cloud import bigquery

logger = logging.getLogger(__name__)

MAILEON_API_BASE = os.environ.get("MAILEON_API_BASE", "https://api.maileon.com/1.0")
GCP_PROJECT = os.environ.get("MAILEON_GCP_PROJECT", "dwh_project")
TRUSTED_DATASET = os.environ.get("MAILEON_TRUSTED_DATASET", "trusted")
STAGING_DATASET = os.environ.get("MAILEON_STAGING_DATASET", "trusted_staging")


def _mailing_ids_from_int_tables(client: bigquery.Client) -> List[Dict[str, Any]]:
    qry = f"""
    WITH a AS (
      SELECT DISTINCT CAST(mailing_id AS STRING) AS mailing_id
      FROM `{GCP_PROJECT}.{TRUSTED_DATASET}.int_maileon_blocks`
      UNION ALL
      SELECT DISTINCT CAST(mailing_id AS STRING) AS mailing_id
      FROM `{GCP_PROJECT}.{TRUSTED_DATASET}.int_maileon_bounces`
      UNION ALL
      SELECT DISTINCT CAST(mailing_id AS STRING) AS mailing_id
      FROM `{GCP_PROJECT}.{TRUSTED_DATASET}.int_maileon_clicks`
      UNION ALL
      SELECT DISTINCT CAST(mailing_id AS STRING) AS mailing_id
      FROM `{GCP_PROJECT}.{TRUSTED_DATASET}.int_maileon_opens`
      UNION ALL
      SELECT DISTINCT CAST(mailing_id AS STRING) AS mailing_id
      FROM `{GCP_PROJECT}.{TRUSTED_DATASET}.int_maileon_unsubscriptions`
      UNION ALL
      SELECT DISTINCT CAST(mailing_id AS STRING) AS mailing_id
      FROM `{GCP_PROJECT}.{TRUSTED_DATASET}.int_maileon_recipients`
      UNION ALL
      SELECT DISTINCT CAST(mailing_id AS STRING) AS mailing_id
      FROM `{GCP_PROJECT}.{TRUSTED_DATASET}.int_maileon_clicks_unique`
      UNION ALL
      SELECT DISTINCT CAST(mailing_id AS STRING) AS mailing_id
      FROM `{GCP_PROJECT}.{TRUSTED_DATASET}.int_maileon_opens_unique`
    )
    SELECT DISTINCT mailing_id FROM a
    """
    return [dict(row) for row in client.query(qry).result()]


def _get_with_retries(
    url: str, headers: Dict[str, str], mailing_id: str
) -> Optional[str]:
    max_retries = 3
    retry_delay = 30

    for retry in range(max_retries):
        try:
            response = requests.get(url, headers=headers, timeout=10)

            if response.status_code == 404:
                logger.warning("mailing_id=%s not found (404)", mailing_id)
                return None
            if response.status_code == 401:
                logger.error("Unauthorized (401) for mailing_id=%s", mailing_id)
                return None
            if response.status_code == 403:
                logger.error("Forbidden (403) for mailing_id=%s", mailing_id)
                return None
            if response.status_code == 415:
                logger.error(
                    "Unsupported media type (415) for mailing_id=%s", mailing_id
                )
                return None
            if response.status_code in (429, 500):
                if retry < max_retries - 1:
                    logger.warning(
                        "HTTP %s for mailing_id=%s — retry in %ss (%s/%s)",
                        response.status_code,
                        mailing_id,
                        retry_delay,
                        retry + 1,
                        max_retries,
                    )
                    time.sleep(retry_delay)
                    retry_delay *= 2
                    continue
                logger.error(
                    "HTTP %s for mailing_id=%s after %s retries",
                    response.status_code,
                    mailing_id,
                    max_retries,
                )
                return None

            response.raise_for_status()
            return response.text

        except requests.exceptions.Timeout:
            if retry < max_retries - 1:
                logger.warning(
                    "Timeout mailing_id=%s — retry in %ss", mailing_id, retry_delay
                )
                time.sleep(retry_delay)
                retry_delay *= 2
                continue
            logger.error("Timeout mailing_id=%s after retries", mailing_id)
            return None
        except requests.exceptions.ConnectionError as e:
            if retry < max_retries - 1:
                logger.warning(
                    "Connection error mailing_id=%s: %s — retry in %ss",
                    mailing_id,
                    e,
                    retry_delay,
                )
                time.sleep(retry_delay)
                retry_delay *= 2
                continue
            logger.error("Connection error mailing_id=%s: %s", mailing_id, e)
            return None
        except requests.exceptions.RequestException as e:
            logger.error("Request error mailing_id=%s: %s", mailing_id, e)
            return None

    return None


def get_maileon_names(
    maileon_api_key: str,
    tmp_loc: str,
    execution_date,
) -> Optional[List[Dict[str, Any]]]:
    """Lookup mailing display names; write names_{date}.json NDJSON."""
    try:
        client = bigquery.Client(project=GCP_PROJECT)
        maileon_data = _mailing_ids_from_int_tables(client)
        logger.info("Found %s mailing_ids for name enrichment", len(maileon_data))

        headers = {
            "Authorization": f"Basic {maileon_api_key}",
            "Accept": "application/vnd.maileon.api+xml",
        }
        final_results: List[Dict[str, Any]] = []

        for i, row in enumerate(maileon_data, 1):
            mailing_id = str(row["mailing_id"])
            logger.info(
                "Processing mailing_id=%s (%s/%s)", mailing_id, i, len(maileon_data)
            )
            url = f"{MAILEON_API_BASE}/mailings/{quote(mailing_id)}/name"
            raw = _get_with_retries(url, headers, mailing_id)
            if not raw:
                continue

            # Vendor returns a bare <name>...</name> XML fragment.
            name = raw.removeprefix("\n<name>").removesuffix("</name>")
            if name:
                final_results.append({"mailing_id": mailing_id, "name": name})
            else:
                logger.warning("No name found for mailing_id=%s", mailing_id)

        os.makedirs(tmp_loc, exist_ok=True)
        file_path = f"{tmp_loc}names_{execution_date}.json"
        with open(file_path, "w") as f:
            for record in final_results:
                f.write(json.dumps(record) + "\n")

        logger.info(
            "Saved %s/%s name records to %s",
            len(final_results),
            len(maileon_data),
            file_path,
        )
        return final_results or None

    except Exception as e:
        logger.error("Unexpected error in get_maileon_names: %s", e)
        return None


def get_maileon_tags(
    maileon_api_key: str,
    tmp_loc: str,
    execution_date,
    mailing_id: Optional[str] = None,
) -> Optional[List[Dict[str, Any]]]:
    """Lookup mailing tags; write tags_{date}.json NDJSON.

    Default source is the names staging table (run names first). Pass
    mailing_id to probe a single campaign during debugging.
    """
    try:
        client = bigquery.Client(project=GCP_PROJECT)

        if mailing_id:
            maileon_data = [{"mailing_id": str(mailing_id)}]
        else:
            qry = f"""
            SELECT DISTINCT mailing_id
            FROM `{GCP_PROJECT}.{STAGING_DATASET}.maileon_names_tbl`
            """
            maileon_data = [dict(row) for row in client.query(qry).result()]

        logger.info("Found %s mailing_ids for tag enrichment", len(maileon_data))

        headers = {
            "Authorization": f"Basic {maileon_api_key}",
            "Accept": "application/vnd.maileon.api+xml; charset=utf-8",
        }
        final_results: List[Dict[str, Any]] = []

        for i, row in enumerate(maileon_data, 1):
            mid = str(row["mailing_id"])
            logger.info(
                "Processing mailing_id=%s (%s/%s)", mid, i, len(maileon_data)
            )
            url = f"{MAILEON_API_BASE}/mailings/{quote(mid)}/settings/tags"
            raw = _get_with_retries(url, headers, mid)
            if raw:
                # Keep raw XML; dbt/downstream parses tag structure.
                final_results.append({"mailing_id": mid, "tags": raw})
            else:
                logger.warning("No tags found for mailing_id=%s", mid)

        os.makedirs(tmp_loc, exist_ok=True)
        file_path = f"{tmp_loc}tags_{execution_date}.json"
        with open(file_path, "w") as f:
            for record in final_results:
                f.write(json.dumps(record) + "\n")

        logger.info(
            "Saved %s/%s tag records to %s",
            len(final_results),
            len(maileon_data),
            file_path,
        )
        return final_results or None

    except Exception as e:
        logger.error("Unexpected error in get_maileon_tags: %s", e)
        return None
