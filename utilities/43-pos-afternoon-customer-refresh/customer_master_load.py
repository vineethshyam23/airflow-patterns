"""GCS → BigQuery staging helpers for POS vendor customer-master files.

Afternoon refresh only touches debtor + location (debloc) dumps. The
overnight POS DAG owns the full table set and hash-based SCD merge;
this module is the selective midday lander.

Source (read-only): helpers inside dags/etl_dish_pos_afternoon.py
"""

from __future__ import annotations

import logging
from typing import List, Optional

from google.cloud import bigquery, storage

logger = logging.getLogger(__name__)

# Vendor object prefixes stay as landed by the export job.
TABLE_PREFIX = {
    "vendor_debtor": "Vendor-Debtor",
    "vendor_location": "Vendor-DebLoc",
}


def schema_definition(tblname: str) -> List[bigquery.SchemaField]:
    """Return the semicolon-CSV schema for a customer-master table."""
    if tblname == "vendor_location":
        return [
            bigquery.SchemaField("Debnr", "STRING"),
            bigquery.SchemaField("Vestcode", "STRING"),
            bigquery.SchemaField("Naam", "STRING"),
            bigquery.SchemaField("Adres", "STRING"),
            bigquery.SchemaField("Postcode", "STRING"),
            bigquery.SchemaField("Woonplaats", "STRING"),
            bigquery.SchemaField("Telefoon", "STRING"),
            bigquery.SchemaField("Hoofdvestiging", "STRING"),
            bigquery.SchemaField("Schonen", "STRING"),
            bigquery.SchemaField("LocatieCodeKlant", "STRING"),
            bigquery.SchemaField("FacturatieUID", "STRING"),
        ]
    if tblname == "vendor_debtor":
        return [
            bigquery.SchemaField("DebNr", "STRING"),
            bigquery.SchemaField("Naam", "STRING"),
            bigquery.SchemaField("Adres", "STRING"),
            bigquery.SchemaField("Postcode", "STRING"),
            bigquery.SchemaField("Woonplaats", "STRING"),
            bigquery.SchemaField("Telefoon", "STRING"),
            bigquery.SchemaField("Fax", "STRING"),
            bigquery.SchemaField("Tav", "STRING"),
            bigquery.SchemaField("Email", "STRING"),
            bigquery.SchemaField("BtwNr", "STRING"),
            bigquery.SchemaField("BtwCode", "STRING"),
            bigquery.SchemaField("BetCond", "STRING"),
            bigquery.SchemaField("Iban", "STRING"),
            bigquery.SchemaField("Bic", "STRING"),
            bigquery.SchemaField("FactDebNr", "STRING"),
        ]
    return []


def get_prefix(tblname: str) -> str:
    return TABLE_PREFIX.get(tblname, "")


def get_filename(
    source_bucket: str,
    tblname: str,
    date_token: str,
) -> Optional[str]:
    """Pick the newest blob whose name contains today's date token.

    Vendor dumps land several times a day under a fixed prefix. We want
    the latest upload for the current calendar day, not the first match
    from list_blobs order.
    """
    prefix = get_prefix(tblname)
    client = storage.Client()
    latest_blob = None
    latest_time = None

    for blob in client.list_blobs(source_bucket, prefix=prefix):
        if date_token not in str(blob.name):
            continue
        blob_time = blob.time_created
        if latest_time is None or blob_time > latest_time:
            latest_time = blob_time
            latest_blob = blob

    if latest_blob is None:
        logger.warning(
            "No blob for table=%s prefix=%s date_token=%s",
            tblname,
            prefix,
            date_token,
        )
        return None

    logger.info(
        "Selected %s (created=%s) for table=%s",
        latest_blob.name,
        latest_time,
        tblname,
    )
    return str(latest_blob.name)


def load_file_staging(
    project_id: str,
    dataset_staging: str,
    source_bucket: str,
    filename: str,
    tblname: str,
) -> None:
    """TRUNCATE-load a semicolon CSV into trusted_staging.<tbl>_stg."""
    client = bigquery.Client()
    table_id = f"{project_id}.{dataset_staging}.{tblname}_stg"
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.CSV,
        schema=schema_definition(tblname),
        skip_leading_rows=1,
        allow_quoted_newlines=True,
        field_delimiter=";",
        allow_jagged_rows=True,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        ignore_unknown_values=True,
        max_bad_records=0,
        quote_character='"',
    )
    uri = f"gs://{source_bucket}/{filename}"
    load_job = client.load_table_from_uri(uri, table_id, job_config=job_config)
    load_job.result()
    table = client.get_table(table_id)
    logger.info("Loaded %s rows into %s", table.num_rows, table_id)


def load_customer_master_table(
    project_id: str,
    dataset_staging: str,
    source_bucket: str,
    tblname: str,
    date_token: str,
) -> None:
    """Resolve today's newest file and load staging. No-op if missing."""
    filename = get_filename(source_bucket, tblname, date_token)
    if not filename:
        logger.info("Skipping %s — no matching file for %s", tblname, date_token)
        return
    load_file_staging(
        project_id=project_id,
        dataset_staging=dataset_staging,
        source_bucket=source_bucket,
        filename=filename,
        tblname=tblname,
    )
