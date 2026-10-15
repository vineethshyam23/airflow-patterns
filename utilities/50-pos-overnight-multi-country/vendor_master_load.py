"""GCS → BigQuery staging for overnight POS vendor master dumps.

Overnight owns the full master set (machines, articles, debtors,
locations, leads, orders). Afternoon pattern 43 only re-lands debtor +
location. Staging is WRITE_TRUNCATE per table; trusted SCD merge and
`_rowhash` / `_keyhash` consumption live in the downstream dbt jobs.

Source (read-only): helpers inside dags/etl_dish_pos.py
"""

from __future__ import annotations

import logging
from typing import Dict, List, Optional

from google.cloud import bigquery, storage

logger = logging.getLogger(__name__)

# Representative overnight set. Production also lands lead_* and order_*
# CSVs under the same prefix contract; schemas omitted here to keep the
# sample focused on the SCD-relevant cores.
MASTER_TABLES = [
    "vendor_machine",
    "vendor_art_class",
    "vendor_article",
    "vendor_location",
    "vendor_debtor",
]

TABLE_PREFIX: Dict[str, str] = {
    "vendor_machine": "Vendor-Machine",
    "vendor_art_class": "Vendor-ArtClass",
    "vendor_article": "Vendor-Article",
    "vendor_location": "Vendor-DebLoc",
    "vendor_debtor": "Vendor-Debtor",
}


def schema_definition(tblname: str) -> List[bigquery.SchemaField]:
    """Semicolon-CSV schemas for the core master tables."""
    if tblname == "vendor_machine":
        return [
            bigquery.SchemaField("MachCode", "STRING"),
            bigquery.SchemaField("Debnr", "STRING"),
            bigquery.SchemaField("VestCode", "STRING"),
            bigquery.SchemaField("DatumIngang", "STRING"),
            bigquery.SchemaField("DatumVerval", "STRING"),
            bigquery.SchemaField("Type", "STRING"),
            bigquery.SchemaField("ArtNr", "STRING"),
            bigquery.SchemaField("SerieNr", "STRING"),
            bigquery.SchemaField("Type_OV", "STRING"),
            bigquery.SchemaField("lease", "STRING"),
            bigquery.SchemaField("lease_start", "STRING"),
            bigquery.SchemaField("lease_eind", "STRING"),
        ]
    if tblname == "vendor_art_class":
        return [
            bigquery.SchemaField("ClassID", "STRING"),
            bigquery.SchemaField("ClassCode", "STRING"),
            bigquery.SchemaField("Omschrijving", "STRING"),
            bigquery.SchemaField("Relatie", "STRING"),
        ]
    if tblname == "vendor_article":
        return [
            bigquery.SchemaField("ARTCODE", "STRING"),
            bigquery.SchemaField("AR_SOORT", "STRING"),
            bigquery.SchemaField("KORT_OMS", "STRING"),
            bigquery.SchemaField("OMS30", "STRING"),
            bigquery.SchemaField("ARTGRP", "STRING"),
            bigquery.SchemaField("CLASS01", "STRING"),
            bigquery.SchemaField("CLASS02", "STRING"),
            bigquery.SchemaField("CLASS03", "STRING"),
        ]
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


def get_filename(source_bucket: str, tblname: str, date_token: str) -> Optional[str]:
    """First blob under the vendor prefix whose name contains date_token.

    Overnight historically took the first list_blobs match. Afternoon
    (#43) improved this to newest-by-time_created for same-day dumps.
    Prefer the afternoon logic if you unify the two DAGs.
    """
    prefix = get_prefix(tblname)
    client = storage.Client()
    for blob in client.list_blobs(source_bucket, prefix=prefix):
        if date_token in str(blob.name):
            return str(blob.name)
    logger.warning(
        "No blob for table=%s prefix=%s date_token=%s",
        tblname,
        prefix,
        date_token,
    )
    return None


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


def load_master_table(
    project_id: str,
    dataset_staging: str,
    source_bucket: str,
    tblname: str,
    date_token: str,
) -> None:
    """Resolve today's dump and load staging. Soft-skip if missing."""
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


def sql_rowhash(tblname: str) -> str:
    """BigQuery expression used by trusted SCD merge (dbt / SQL layer).

    Kept here so the Composer lander and warehouse models share one
    definition of "row changed". Empty string = table not hash-tracked
    in this sample (leads/orders in production follow the same idea).
    """
    if tblname == "vendor_machine":
        return (
            "TO_HEX(MD5(CONCAT("
            "IFNULL(CAST(MachCode AS STRING),''), "
            "IFNULL(CAST(Debnr AS STRING),''), "
            "IFNULL(CAST(VestCode AS STRING),''), "
            "IFNULL(CAST(DatumIngang AS STRING),''), "
            "IFNULL(CAST(DatumVerval AS STRING),''), "
            "IFNULL(CAST(Type AS STRING),''), "
            "IFNULL(CAST(ArtNr AS STRING),''), "
            "IFNULL(CAST(SerieNr AS STRING),''), "
            "IFNULL(CAST(Type_OV AS STRING),''), "
            "IFNULL(CAST(lease AS STRING),''), "
            "IFNULL(CAST(lease_start AS STRING),''), "
            "IFNULL(CAST(lease_eind AS STRING),'')"
            "))) AS _rowhash"
        )
    if tblname == "vendor_art_class":
        return (
            "TO_HEX(MD5(CONCAT("
            "IFNULL(CAST(ClassID AS STRING),''), "
            "IFNULL(CAST(ClassCode AS STRING),''), "
            "IFNULL(CAST(Omschrijving AS STRING),''), "
            "IFNULL(CAST(Relatie AS STRING),'')"
            "))) AS _rowhash"
        )
    if tblname == "vendor_article":
        return (
            "TO_HEX(MD5(CONCAT("
            "IFNULL(CAST(ARTCODE AS STRING),''), "
            "IFNULL(CAST(AR_SOORT AS STRING),''), "
            "IFNULL(CAST(KORT_OMS AS STRING),''), "
            "IFNULL(CAST(OMS30 AS STRING),''), "
            "IFNULL(CAST(ARTGRP AS STRING),''), "
            "IFNULL(CAST(CLASS01 AS STRING),''), "
            "IFNULL(CAST(CLASS02 AS STRING),''), "
            "IFNULL(CAST(CLASS03 AS STRING),'')"
            "))) AS _rowhash"
        )
    if tblname == "vendor_location":
        return (
            "TO_HEX(MD5(CONCAT("
            "IFNULL(CAST(Debnr AS STRING),''), "
            "IFNULL(CAST(Vestcode AS STRING),''), "
            "IFNULL(CAST(Naam AS STRING),''), "
            "IFNULL(CAST(Adres AS STRING),''), "
            "IFNULL(CAST(Postcode AS STRING),''), "
            "IFNULL(CAST(Woonplaats AS STRING),''), "
            "IFNULL(CAST(Telefoon AS STRING),''), "
            "IFNULL(CAST(Hoofdvestiging AS STRING),''), "
            "IFNULL(CAST(Schonen AS STRING),''), "
            "IFNULL(CAST(LocatieCodeKlant AS STRING),''), "
            "IFNULL(CAST(FacturatieUID AS STRING),'')"
            "))) AS _rowhash"
        )
    if tblname == "vendor_debtor":
        return (
            "TO_HEX(MD5(CONCAT("
            "IFNULL(CAST(DebNr AS STRING),''), "
            "IFNULL(CAST(Naam AS STRING),''), "
            "IFNULL(CAST(Adres AS STRING),''), "
            "IFNULL(CAST(Postcode AS STRING),''), "
            "IFNULL(CAST(Woonplaats AS STRING),''), "
            "IFNULL(CAST(Telefoon AS STRING),''), "
            "IFNULL(CAST(Fax AS STRING),''), "
            "IFNULL(CAST(Tav AS STRING),''), "
            "IFNULL(CAST(Email AS STRING),''), "
            "IFNULL(CAST(BtwNr AS STRING),''), "
            "IFNULL(CAST(BtwCode AS STRING),''), "
            "IFNULL(CAST(BetCond AS STRING),''), "
            "IFNULL(CAST(Iban AS STRING),''), "
            "IFNULL(CAST(Bic AS STRING),''), "
            "IFNULL(CAST(FactDebNr AS STRING),'')"
            "))) AS _rowhash"
        )
    return ""


def sql_keyhash(tblname: str) -> str:
    """Natural-key hash for SCD match / dedupe."""
    if tblname == "vendor_machine":
        return "TO_HEX(MD5(CONCAT(IFNULL(CAST(MachCode AS STRING),'')))) AS _keyhash"
    if tblname == "vendor_art_class":
        return (
            "TO_HEX(MD5(CONCAT("
            "IFNULL(CAST(ClassID AS STRING),''), "
            "IFNULL(CAST(ClassCode AS STRING),'')"
            "))) AS _keyhash"
        )
    if tblname == "vendor_article":
        return "TO_HEX(MD5(CONCAT(IFNULL(CAST(ARTCODE AS STRING),'')))) AS _keyhash"
    if tblname == "vendor_location":
        return (
            "TO_HEX(MD5(CONCAT("
            "IFNULL(CAST(Debnr AS STRING),''), "
            "IFNULL(CAST(Vestcode AS STRING),'')"
            "))) AS _keyhash"
        )
    if tblname == "vendor_debtor":
        return "TO_HEX(MD5(CONCAT(IFNULL(CAST(DebNr AS STRING),'')))) AS _keyhash"
    return ""
