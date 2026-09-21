"""Adobe Analytics *app* Data Feed extract helpers for Composer.

Same dual-payload contract as the web hourly lander (pattern 49), but
the landing prefix and Composer data folder target the mobile-app
report suite instead of the web suite.

Landing zone drops either:
  - ``*.tar.gz`` — lean lookup dumps (connection, country, language, OS)
  - ``*.tsv.gz`` — hit-level TSV for the app report suite

Public entry points used by the DAG:
  process_tar_files()
  process_tsv_gz_files()
"""

from __future__ import annotations

import gzip
import logging
import os
import re
import shutil
import tarfile
from typing import List

from google.cloud import storage

logger = logging.getLogger(__name__)

SOURCE_BUCKET_NAME = os.environ.get("AA_LANDING_BUCKET", "landingzone")
DESTINATION_BUCKET_NAME = os.environ.get("AA_COMPOSER_BUCKET", "composer-data")
LANDING_PREFIX = "adobe-rawfeed-app/"
DESTINATION_FOLDER = "data/adobe-rawfeed-app/"
PROCESSED_FOLDER = "adobe-rawfeed-app/processed/"
TEMP_DIR = "/tmp/gcs_extracted_app/"

_PROCESSED_MARKERS = ("processed/",)


def _client() -> storage.Client:
    return storage.Client()


def list_files(pattern: str, source_bucket: str = SOURCE_BUCKET_NAME) -> List[str]:
    """List landing-zone objects matching ``pattern``, skip processed prefixes."""
    bucket = _client().bucket(source_bucket)
    matches: List[str] = []
    for blob in bucket.list_blobs(prefix=LANDING_PREFIX):
        if any(marker in blob.name for marker in _PROCESSED_MARKERS):
            continue
        if re.search(pattern, blob.name):
            matches.append(blob.name)
    return matches


def move_file_to_processed(
    blob_name: str,
    source_bucket: str = SOURCE_BUCKET_NAME,
) -> str:
    """Copy landing object into processed prefix, then delete the original."""
    bucket = _client().bucket(source_bucket)
    source_blob = bucket.blob(blob_name)
    new_blob_name = f"{PROCESSED_FOLDER}{os.path.basename(blob_name)}"
    bucket.copy_blob(source_blob, bucket, new_blob_name)
    source_blob.delete()
    logger.info("Moved %s → %s", blob_name, new_blob_name)
    return new_blob_name


def process_tar_file(
    blob_name: str,
    source_bucket: str = SOURCE_BUCKET_NAME,
    dest_bucket_name: str = DESTINATION_BUCKET_NAME,
) -> None:
    """Download one ``.tar.gz``, extract members, upload with suite-stem suffix."""
    os.makedirs(TEMP_DIR, exist_ok=True)
    source_basename = blob_name.replace(".tar.gz", "")
    local_tar_path = os.path.join(TEMP_DIR, os.path.basename(blob_name))
    extract_folder = os.path.join(TEMP_DIR, os.path.basename(source_basename))

    bucket = _client().bucket(source_bucket)
    bucket.blob(blob_name).download_to_filename(local_tar_path)

    os.makedirs(extract_folder, exist_ok=True)
    with tarfile.open(local_tar_path, "r:gz") as tar:
        tar.extractall(extract_folder)

    dest_bucket = _client().bucket(dest_bucket_name)
    for root, _, files in os.walk(extract_folder):
        for filename in files:
            file_name, file_ext = os.path.splitext(filename)
            new_filename = f"{file_name}_{os.path.basename(source_basename)}{file_ext}"
            destination_blob_name = f"{DESTINATION_FOLDER}{new_filename}"
            dest_bucket.blob(destination_blob_name).upload_from_filename(
                os.path.join(root, filename)
            )
            logger.info("Uploaded %s", destination_blob_name)

    move_file_to_processed(blob_name, source_bucket=source_bucket)

    os.remove(local_tar_path)
    for name in os.listdir(extract_folder):
        os.remove(os.path.join(extract_folder, name))
    os.rmdir(extract_folder)


def process_tsv_gz_file(
    blob_name: str,
    source_bucket: str = SOURCE_BUCKET_NAME,
    dest_bucket_name: str = DESTINATION_BUCKET_NAME,
) -> None:
    """Download one ``.tsv.gz``, gunzip locally, upload ``.tsv`` to Composer data."""
    os.makedirs(TEMP_DIR, exist_ok=True)
    source_basename = blob_name.replace(".tsv.gz", "")
    local_gz_path = os.path.join(TEMP_DIR, os.path.basename(blob_name))
    local_tsv_path = local_gz_path.replace(".gz", "")

    bucket = _client().bucket(source_bucket)
    bucket.blob(blob_name).download_to_filename(local_gz_path)

    with gzip.open(local_gz_path, "rb") as f_in, open(local_tsv_path, "wb") as f_out:
        shutil.copyfileobj(f_in, f_out)

    new_filename = f"{os.path.basename(source_basename)}.tsv"
    destination_blob_name = f"{DESTINATION_FOLDER}{new_filename}"
    _client().bucket(dest_bucket_name).blob(destination_blob_name).upload_from_filename(
        local_tsv_path
    )
    logger.info("Uploaded %s", destination_blob_name)

    move_file_to_processed(blob_name, source_bucket=source_bucket)
    os.remove(local_gz_path)
    os.remove(local_tsv_path)


def process_tar_files(
    source_bucket: str = SOURCE_BUCKET_NAME,
    dest_bucket_name: str = DESTINATION_BUCKET_NAME,
) -> int:
    """Process every unprocessed ``.tar.gz`` under the app landing prefix."""
    tar_files = list_files(r".*\.tar\.gz$", source_bucket=source_bucket)
    for blob_name in tar_files:
        process_tar_file(
            blob_name,
            source_bucket=source_bucket,
            dest_bucket_name=dest_bucket_name,
        )
    return len(tar_files)


def process_tsv_gz_files(
    source_bucket: str = SOURCE_BUCKET_NAME,
    dest_bucket_name: str = DESTINATION_BUCKET_NAME,
) -> int:
    """Process every unprocessed ``.tsv.gz`` under the app landing prefix."""
    tsv_files = list_files(r".*\.tsv\.gz$", source_bucket=source_bucket)
    for blob_name in tsv_files:
        process_tsv_gz_file(
            blob_name,
            source_bucket=source_bucket,
            dest_bucket_name=dest_bucket_name,
        )
    return len(tsv_files)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print("tar.gz candidates:", list_files(r".*\.tar\.gz$"))
    print("tsv.gz candidates:", list_files(r".*\.tsv\.gz$"))
