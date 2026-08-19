"""
Freshdesk REST extract client.

Paginates /api/v2/{resource} into NDJSON on a local/Composer path.
Tickets use updated_since = first day of the current calendar month
so hourly runs stay bounded; contacts/companies stringify custom_fields
because nested JSON breaks flat BQ schema objects.

API key comes from Airflow Variables at call time — never hard-code it.
"""

from __future__ import annotations

import json
import logging
import time
from datetime import date
from typing import Any

import requests


class FreshdeskClient:
    """Thin wrapper around Freshdesk list endpoints."""

    def __init__(self, api_key: str, domain: str, project_id: str | None = None):
        self.api_key = api_key
        self.domain = domain
        self.project_id = project_id
        self.headers = {"Content-Type": "application/json"}

    def _base_url(self, url_suffix: str) -> str:
        return f"https://{self.domain}.freshdesk.com/api/v2/{url_suffix}"

    def endpoint(self, url_suffix: str, temp_loc: str) -> None:
        """
        Page through a list endpoint and write NDJSON to
        ``{temp_loc}{url_suffix}.json``.

        On HTTP != 200 we stop the page loop (caller retries via Airflow).
        Rate-limit / transient exceptions sleep briefly then exit — the
        next DAG retry restarts from page 1 with WRITE overwrite.
        """
        page = 1
        params: dict[str, Any] = {"page": page, "per_page": 100}

        if url_suffix == "tickets":
            today = date.today()
            month_start = date(today.year, today.month, 1)
            params["updated_since"] = month_start.strftime("%Y-%m-%dT%H:%M:%SZ")

        out_path = f"{temp_loc}{url_suffix}.json"
        url = self._base_url(url_suffix)

        try:
            with open(out_path, "w", encoding="utf-8") as outfile:
                logging.info("Getting %s", url_suffix)
                while True:
                    params["page"] = page
                    logging.info("Fetching page #%s from %s", page, url_suffix)

                    response = requests.get(
                        url,
                        auth=(self.api_key, "X"),
                        headers=self.headers,
                        params=params,
                        timeout=60,
                    )

                    if response.status_code != 200:
                        logging.error(
                            "Error fetching %s: %s - %s",
                            url,
                            response.status_code,
                            response.text[:500],
                        )
                        break

                    data = response.json()
                    if not data:
                        logging.info("Fetch complete for %s", url_suffix)
                        break

                    for record in data:
                        # custom_fields as nested dicts break schema_json
                        # flat loads for companies/contacts — stringify.
                        if url_suffix in ("companies", "contacts"):
                            row = {
                                key: (
                                    str(value)
                                    if key == "custom_fields"
                                    else value
                                )
                                for key, value in record.items()
                            }
                        else:
                            row = record
                        outfile.write(json.dumps(row) + "\n")

                    page += 1

        except Exception as exc:
            # Freshdesk returns 429 under burst; Airflow retries the task.
            logging.warning(
                "Exception during %s fetch (will rely on task retry): %s",
                url_suffix,
                exc,
            )
            time.sleep(0.5)
            raise
