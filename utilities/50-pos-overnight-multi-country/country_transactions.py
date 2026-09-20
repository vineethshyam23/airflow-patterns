"""Multi-country POS transaction + tenant-debtor mapping landers.

Each country lands under:
  transactions_daily/{country}/{path}orders/tickets-{country}-{YYYYMMDD}.jsonl

NL historically had no path version prefix; DE/FR/IT/ES use v2/.
Staging tables hold a single JSON column (`value`) so evolving ticket
payloads do not force Composer schema churn — dbt unpacks downstream.

Mapping dumps:
  transactions_daily/{country}/cm/Tenant_Debtor_{YYYYMMDD}.jsonl
→ trusted.vendor_customer_transaction_mapping_{ISO}

Source (read-only): load/move helpers in dags/etl_dish_pos.py
"""

from __future__ import annotations

import logging
from typing import List, Optional, Tuple

from google.cloud import bigquery, storage

from date_range import get_date_range

logger = logging.getLogger(__name__)

# (iso, folder slug, object path prefix under country/)
# path "" = NL legacy layout; "v2/" = later country rollouts.
COUNTRY_SPECS: List[Tuple[str, str, str]] = [
    ("NL", "netherlands", ""),
    ("DE", "germany", "v2/"),
    ("FR", "france", "v2/"),
    ("IT", "italy", "v2/"),
    ("ES", "spain", "v2/"),
]


def load_transaction_files_for_date_range(
    project_id: str,
    dataset_staging: str,
    source_bucket: str,
    iso_code: str,
    country: str,
    path: str,
    backfill_start: Optional[str] = None,
    backfill_end: Optional[str] = None,
) -> None:
    """APPEND each day's ticket JSONL into staging. Soft-skip missing days."""
    date_list = get_date_range(backfill_start, backfill_end)
    bq = bigquery.Client()
    gcs = storage.Client()
    bucket = gcs.bucket(source_bucket)
    schema_fields = [bigquery.SchemaField("value", "JSON", mode="NULLABLE")]
    table_id = f"{project_id}.{dataset_staging}.pos_transactions_{iso_code}"

    for date_str in date_list:
        prefix = (
            f"transactions_daily/{country}/{path}orders/"
            f"tickets-{country}-{date_str}"
        )
        blobs = list(bucket.list_blobs(prefix=prefix))
        if not blobs:
            logger.info("No tickets for %s on %s (prefix=%s)", iso_code, date_str, prefix)
            continue

        for blob in blobs:
            if blob.name.endswith("/"):
                continue
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.CSV,
                schema=schema_fields,
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
                field_delimiter="\t",
                skip_leading_rows=0,
                autodetect=False,
            )
            uri = f"gs://{source_bucket}/{blob.name}"
            try:
                load_job = bq.load_table_from_uri(uri, table_id, job_config=job_config)
                load_job.result()
                logger.info("Loaded %s → %s", blob.name, table_id)
            except Exception as exc:  # noqa: BLE001 — continue other dates/files
                logger.error("Failed %s: %s", blob.name, exc)


def move_transaction_files_for_date_range(
    source_bucket: str,
    iso_code: str,
    country: str,
    path: str,
    backfill_start: Optional[str] = None,
    backfill_end: Optional[str] = None,
) -> None:
    """Copy+delete ticket objects into .../orders/processed/ after dbt succeeds."""
    date_list = get_date_range(backfill_start, backfill_end)
    gcs = storage.Client()
    bucket = gcs.bucket(source_bucket)

    for date_str in date_list:
        prefix = (
            f"transactions_daily/{country}/{path}orders/"
            f"tickets-{country}-{date_str}"
        )
        for blob in bucket.list_blobs(prefix=prefix):
            if blob.name.endswith("/"):
                continue
            filename = blob.name.rsplit("/", 1)[-1]
            dest = (
                f"transactions_daily/{country}/{path}orders/processed/{filename}"
            )
            try:
                bucket.copy_blob(blob, bucket, dest)
                blob.delete()
                logger.info("Moved %s → %s (%s)", blob.name, dest, iso_code)
            except Exception as exc:  # noqa: BLE001
                logger.error("Move failed %s: %s", blob.name, exc)


def load_mapping_file(
    project_id: str,
    dataset_trusted: str,
    gcs_uri: str,
    iso_code: str,
) -> None:
    """TRUNCATE-load one Tenant_Debtor JSONL into trusted mapping table."""
    schema_def = [
        bigquery.SchemaField("tenant_id", "STRING"),
        bigquery.SchemaField("debtor_number", "STRING"),
        bigquery.SchemaField("name", "STRING"),
        bigquery.SchemaField("id", "INTEGER"),
    ]
    client = bigquery.Client()
    table_id = (
        f"{project_id}.{dataset_trusted}."
        f"vendor_customer_transaction_mapping_{iso_code}"
    )
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        schema=schema_def,
        allow_quoted_newlines=True,
        allow_jagged_rows=True,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        ignore_unknown_values=True,
        max_bad_records=0,
    )
    load_job = client.load_table_from_uri(gcs_uri, table_id, job_config=job_config)
    load_job.result()
    table = client.get_table(table_id)
    logger.info("Loaded %s mapping rows into %s", table.num_rows, table_id)


def load_mapping_files_for_date_range(
    project_id: str,
    dataset_trusted: str,
    source_bucket: str,
    iso_code: str,
    country: str,
    backfill_start: Optional[str] = None,
    backfill_end: Optional[str] = None,
) -> None:
    """Walk date range; last successful day wins (TRUNCATE per file)."""
    date_list = get_date_range(backfill_start, backfill_end)
    for file_timestamp in date_list:
        uri = (
            f"gs://{source_bucket}/transactions_daily/{country}/cm/"
            f"Tenant_Debtor_{file_timestamp}.jsonl"
        )
        try:
            load_mapping_file(project_id, dataset_trusted, uri, iso_code)
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "Mapping load skipped %s %s: %s",
                iso_code,
                file_timestamp,
                exc,
            )
