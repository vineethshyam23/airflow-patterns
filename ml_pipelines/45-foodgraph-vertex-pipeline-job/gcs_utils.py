"""GCS helpers for Vertex pipeline template I/O.

Production used Airflow's GCP connection extra to materialise a
service-account keyfile for google-cloud-storage. This reference keeps
the same shape but never logs key material and prefers ADC when no
connection JSON is supplied.

Source (read-only): dags/horeca_digital/food_graph_vertex_utils.py
"""

from __future__ import annotations

import json
import logging
import os
import random
import sys
import tempfile
from pathlib import Path
from urllib.parse import urlparse

from google.cloud import storage
from google.oauth2 import service_account

log = logging.getLogger(__name__)

DEFAULT_GCS_PROJECT = "vertex_ml_project"


def _configure_logging() -> None:
    if not logging.getLogger().handlers:
        logging.basicConfig(
            stream=sys.stdout,
            level=logging.INFO,
            format="%(asctime)s %(levelname)s %(filename)s:%(lineno)d %(message)s",
        )


def initialize_client(is_dev_run: bool, connection_json: dict | None, project: str):
    """Build a GCS client from Airflow connection extras or ADC.

    ``connection_json`` is typically ``BaseHook.get_connection(...).extra_dejson``.
    We accept either ``extra__google_cloud_platform__key_path`` or
    ``extra__google_cloud_platform__keyfile_dict``. Never log the key body.
    """
    _configure_logging()
    connection_json = connection_json or {}

    if is_dev_run:
        # Local laptop path — not used on Composer.
        key_path = os.path.expanduser("~/.config/gcloud/application_default_credentials.json")
        if os.path.exists(key_path):
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_path
    else:
        key_path = connection_json.get("extra__google_cloud_platform__key_path", "") or ""
        keyfile_json_str = (
            connection_json.get("extra__google_cloud_platform__keyfile_dict", "") or ""
        )
        if key_path:
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_path
        elif keyfile_json_str:
            # Write SA JSON to a temp file for the Google auth libraries.
            # Production leaked key material into logs; do not repeat that.
            service_key = tempfile.NamedTemporaryFile(delete=False, suffix=".json")
            if isinstance(keyfile_json_str, dict):
                service_key.write(json.dumps(keyfile_json_str).encode("utf-8"))
            else:
                service_key.write(str(keyfile_json_str).encode("utf-8"))
            service_key.close()
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = service_key.name
        else:
            log.info("No key_path/keyfile_dict in connection; using application default credentials")

    creds_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if creds_path and os.path.exists(creds_path):
        credentials = service_account.Credentials.from_service_account_file(
            creds_path,
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
        return storage.Client(project=project, credentials=credentials)

    return storage.Client(project=project)


class CloudStorageUtil:
    """Thin static wrappers used by the Vertex pipeline scheduler."""

    @staticmethod
    def download_to_location(
        project: str = "",
        source_file: str = "",
        destination_file_name: str = "",
        connection_json=None,
    ) -> str:
        """Download ``gs://bucket/object`` to a local path; create parents."""
        parsed = urlparse(source_file, allow_fragments=False)
        bucket_name = parsed.netloc
        blob_name = parsed.path.lstrip("/")

        destination_path = Path(destination_file_name)
        destination_path.parent.mkdir(parents=True, exist_ok=True)

        log.info("Downloading gs://%s/%s → %s", bucket_name, blob_name, destination_path)

        client = initialize_client(
            is_dev_run=False,
            connection_json=connection_json,
            project=project or DEFAULT_GCS_PROJECT,
        )
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.download_to_filename(str(destination_path))
        return str(destination_path)

    @staticmethod
    def upload_blob(
        source_file_name: str = "",
        destination_bucket_name: str = "",
        version: str = "",
        destination_blob_name: str = "",
        model_name: str = "",
        connection_json=None,
        project: str = "",
    ) -> str:
        """Upload a local file under ``model_name[/version]/blob`` and return gs:// URI."""
        client = initialize_client(
            is_dev_run=False,
            connection_json=connection_json,
            project=project or DEFAULT_GCS_PROJECT,
        )
        if version:
            destination_filename = f"{model_name}/{version}/{destination_blob_name}"
        else:
            destination_filename = f"{model_name}/{destination_blob_name}"

        bucket = client.bucket(destination_bucket_name)
        blob = bucket.blob(destination_filename)
        blob.upload_from_filename(source_file_name)
        uri = f"gs://{blob.bucket.name}/{blob.name}"
        log.info("Uploaded %s → %s", source_file_name, uri)
        return uri

    @staticmethod
    def download_random_sample_files(
        bucket_name: str = "",
        bucket_directory: str = "",
        local_directory: str = "",
        sample_number: int = 2,
        connection_json=None,
        project: str = "",
    ) -> None:
        """Pull a random sample of objects under a GCS prefix (debug / QA)."""
        client = initialize_client(
            is_dev_run=False,
            connection_json=connection_json,
            project=project or DEFAULT_GCS_PROJECT,
        )
        bucket = client.bucket(bucket_name)
        blobs = list(bucket.list_blobs(prefix=bucket_directory))
        if not blobs:
            log.warning("No objects under gs://%s/%s", bucket_name, bucket_directory)
            return

        n = min(sample_number, len(blobs))
        sample = random.sample(blobs, n)
        Path(local_directory).mkdir(parents=True, exist_ok=True)
        for blob in sample:
            name = blob.name.rsplit("/", 1)[-1]
            if not name:
                continue
            target = str(Path(local_directory) / name)
            blob.download_to_filename(target)
            log.info("Sampled %s → %s", blob.name, target)
