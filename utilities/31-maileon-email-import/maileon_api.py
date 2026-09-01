"""Maileon REST client: XML reports → JSONL on Composer data volume.

Fetches /reports/{type} with a /count preflight, pages at 1000 rows,
converts XML (xmltodict) into flat contact + event fields, and writes
one JSONL line per record under the local path. The DAG then branches
on empty vs non-empty blobs before copying into rawzone.

Source (read-only): dags/horeca_digital/maileon.py
"""

from __future__ import annotations

import json
import logging
import math
import os
import re
from datetime import datetime
from typing import Any, Dict, Iterator, List, Optional

import requests
from google.cloud import storage

logger = logging.getLogger(__name__)


class MaileonAPI:
    def __init__(self, api_key: str, bucket_name: str, project_id: str):
        self.api_key = api_key
        self.base_url = os.environ.get(
            "MAILEON_API_BASE", "https://api.maileon.com/1.0"
        )
        self.headers = {
            "Authorization": f"Basic {self.api_key}",
            "Content-Type": "application/json",
        }
        self.bucket_name = bucket_name
        self.project_id = project_id
        self.storage_client = storage.Client(project=project_id)
        self.bucket = self.storage_client.bucket(bucket_name)

    def _process_xml_response(self, response_text: str) -> Dict:
        """Parse vendor XML and normalize records into a list under 'records'."""
        import xmltodict

        try:
            xml_dict = xmltodict.parse(response_text)

            def clean_nil_values(obj):
                if isinstance(obj, dict):
                    if "@nil" in obj and obj["@nil"] == "true":
                        return None

                    cleaned = {
                        k: clean_nil_values(v)
                        for k, v in obj.items()
                        if not (k.startswith("@") and k != "@nil")
                    }

                    if "response" in cleaned:
                        response = cleaned["response"]
                        for container, item_key in (
                            ("contacts", "contact"),
                            ("clicks", "click"),
                            ("opens", "open"),
                            ("bounces", "bounce"),
                            ("blocks", "block"),
                            ("unsubscriptions", "unsubscription"),
                        ):
                            if container in response and isinstance(
                                response[container], dict
                            ):
                                return {
                                    "records": response[container].get(item_key, [])
                                }
                    return cleaned

                if isinstance(obj, list):
                    return [clean_nil_values(item) for item in obj]
                return obj

            cleaned_dict = clean_nil_values(xml_dict)

            if "records" in cleaned_dict:
                if cleaned_dict["records"] is None:
                    cleaned_dict["records"] = []
                elif not isinstance(cleaned_dict["records"], list):
                    cleaned_dict["records"] = [cleaned_dict["records"]]
            else:
                cleaned_dict["records"] = []

            return cleaned_dict

        except Exception as e:
            logger.error("Failed to parse XML response: %s", e)
            return {"records": []}

    def _make_request(
        self, endpoint: str, params: Optional[Dict[str, Any]] = None
    ) -> Iterator[Optional[Dict]]:
        """Page through an endpoint. Yields each page dict or None on hard fail."""
        if params is None:
            params = {}

        default_params = {"page_index": 1, "page_size": 1000}
        self.headers.update(
            {
                "Accept": "application/vnd.maileon.api+xml",
                "Content-Type": "application/xml",
            }
        )
        params = {**default_params, **params}

        try:
            count_response = requests.get(
                f"{self.base_url}{endpoint}/count",
                headers=self.headers,
                params=params,
                timeout=60,
            )
            count_response.raise_for_status()
            count_match = re.search(r"<count>(\d+)</count>", count_response.text)
            if count_match:
                total_records = int(count_match.group(1))
                logger.info("Total records: %s", total_records)
            else:
                logger.warning(
                    "Could not extract count from response: %s", count_response.text
                )
                total_records = 0

            total_pages = math.ceil(total_records / params["page_size"]) if total_records else 0

            for current_page in range(1, total_pages + 1):
                params["page_index"] = current_page
                logger.info("Fetching page %s of %s", current_page, total_pages)

                response = requests.get(
                    f"{self.base_url}{endpoint}",
                    headers=self.headers,
                    params=params,
                    timeout=120,
                )
                response.raise_for_status()
                yield self._process_xml_response(response.text)

            logger.info("Completed fetching all %s pages", total_pages)

        except requests.exceptions.RequestException as e:
            logger.error("Error making request to %s: %s", endpoint, e)
            if getattr(e, "response", None) is not None:
                logger.error("Response content: %s", e.response.text)
            yield None

    def process_endpoint(
        self, endpoint_name: str, data_getter, local_path: str
    ) -> str:
        """Stream pages into a dated JSONL file under local_path. Returns filename."""
        try:
            data_date = datetime.now().strftime("%Y%m%d")
            filename = f"{endpoint_name}_{data_date}.jsonl"
            out_path = local_path + filename
            os.makedirs(local_path, exist_ok=True)

            # process_endpoint walks the raw XML container keys, not the
            # normalized 'records' list from _process_xml_response — keep
            # both shapes so a vendor payload change does not blank the file.
            endpoint_mapping = {
                "clicks": {"container": "clicks", "record": "click"},
                "clicks_unique": {"container": "clicks", "record": "click"},
                "opens": {"container": "opens", "record": "open"},
                "opens_unique": {"container": "opens", "record": "open"},
                "bounces": {"container": "bounces", "record": "bounce"},
                "blocks": {"container": "blocks", "record": "block"},
                "unsubscriptions": {
                    "container": "unsubscriptions",
                    "record": "unsubscription",
                },
                "recipients": {"container": "recipients", "record": "recipient"},
            }

            record_count = 0
            with open(out_path, "w") as f:
                for page_data in data_getter():
                    if not page_data:
                        continue

                    mapping = endpoint_mapping.get(
                        endpoint_name,
                        {"container": endpoint_name, "record": endpoint_name},
                    )

                    # Prefer normalized records; fall back to vendor containers.
                    records = page_data.get("records")
                    if records is None:
                        container = page_data.get(mapping["container"], {})
                        if isinstance(container, dict):
                            records = container.get(mapping["record"], [])
                        else:
                            records = []

                    if not isinstance(records, list):
                        records = [records] if records else []

                    for record in records:
                        mapped = self._map_schema(record, endpoint_name)
                        f.write(json.dumps(mapped) + "\n")
                        record_count += 1

                    logger.info(
                        "Processed %s records from current page", len(records)
                    )

            logger.info(
                "Successfully processed total %s records to %s",
                record_count,
                filename,
            )
            return filename

        except Exception as e:
            logger.error("Error processing endpoint %s: %s", endpoint_name, e)
            return ""

    def _map_schema(self, data: Dict, report_type: str) -> Dict:
        mapped_data: Dict[str, Any] = {}

        if "contact" in data:
            contact = data["contact"] or {}
            mapped_data.update(
                {
                    "contact_id": contact.get("id") or None,
                    "contact_email": str(contact.get("email", "")),
                    "contact_permissionStatus": str(
                        contact.get("permissionStatus", "")
                    ),
                    "contact_permissionType": str(
                        contact.get("permissionType", "")
                    ),
                    "contact_created": contact.get("created") or None,
                    "contact_updated": str(contact.get("updated", "")),
                    "contact_external_id": str(contact.get("external_id", "")),
                    "contact_standard_fields": str(
                        contact.get("standard_fields", "")
                    ),
                    "contact_custom_fields": str(
                        contact.get("custom_fields", "")
                    ),
                }
            )

        for field in ("timestamp", "mailing_id", "msg_id"):
            if field in data:
                mapped_data[field] = str(data[field])

        if report_type in (
            "opens",
            "opens_unique",
            "clicks",
            "clicks_unique",
        ):
            if "format" in data:
                mapped_data["format"] = str(data["format"])
            if "device_type" in data:
                mapped_data["device_type"] = str(data["device_type"])
            if "count" in data:
                mapped_data["count"] = int(data["count"])

        if report_type in ("clicks", "clicks_unique"):
            for field in ("link_id", "link_type", "link_url"):
                if field in data:
                    mapped_data[field] = str(data[field])

        if report_type == "bounces":
            for field in ("type", "status_code", "source"):
                if field in data:
                    mapped_data[field] = str(data[field])

        if report_type == "blocks":
            for field in ("old_status", "new_status", "reason"):
                if field in data:
                    mapped_data[field] = str(data[field])

        if report_type == "unsubscriptions" and "source" in data:
            mapped_data["source"] = str(data["source"])

        return mapped_data


def import_maileon_data(
    api_key: str,
    bucket_loc: str,
    project_id: str,
    report_type: str,
    endpoint: str,
    local_path: str,
) -> None:
    """Airflow callable: pull one report type into Composer data/{report}/."""
    maileon = MaileonAPI(api_key, bucket_loc, project_id)

    def get_data():
        return maileon._make_request(endpoint)

    maileon.process_endpoint(report_type, get_data, local_path)
