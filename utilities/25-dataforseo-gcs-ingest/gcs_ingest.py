"""SEO listing GCS ingest helpers for Airflow.

Bucket layout (e.g. gs://seo-listings-ingest/):
  uploads/       — landing zone (.json or .json.gz from vendor or manual upload)
  archive_raw/       — vendor bytes preserved ({stem}.json.gz or {stem}.json)
  stg_to_load/       — uncompressed NDJSON pending BQ load (uncompressed_{stem}.json)
  archive_ingested/  — flat archive after successful load (uncompressed_{stem}.json)

Public entry points (called from etl_seo_listings_ingestion DAG):
  ingest_all_uploads()      — stream uploads/ → stg_to_load/ + archive_raw/
  archive_all_stg_to_load() — move stg_to_load/ objects to archive_ingested/ after BQ load

Local / GCS CLI:
  python gcs_ingest.py scan /path/to/dump.json.gz
  python gcs_ingest.py ingest --bucket seo-listings-ingest
  python gcs_ingest.py ingest --bucket seo-listings-ingest --apply
  python gcs_ingest.py archive --bucket seo-listings-ingest --apply

Ingest/archive default to dry-run (JSON preview). Pass --apply to mutate GCS.

Stem: {YYYYMMDD}_countries-{n}_{raw_md5[:8]} from vendor object GCS md5_hash.
Compression detected via magic bytes (\\x1f\\x8b), not filename.
Custom metadata keys map to GCS x-goog-meta-* (Python client omits the prefix).
"""

from __future__ import annotations

import argparse
import base64
import gzip
import hashlib
import json
import logging
import sys
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import BinaryIO, Iterable, Iterator, Optional, Set

from google.cloud import storage

logger = logging.getLogger(__name__)

# GCS prefix paths within the ingest bucket
PREFIX_UPLOADS = "uploads/"
PREFIX_ARCHIVE_RAW = "archive_raw/"
PREFIX_STG_TO_LOAD = "stg_to_load/"
PREFIX_ARCHIVE_INGESTED = "archive_ingested/"

# Deprecated aliases (pre-Option-A rename)
PREFIX_RAW_ARCHIVE = PREFIX_ARCHIVE_RAW
PREFIX_STG = PREFIX_STG_TO_LOAD
PREFIX_ARCHIVE = PREFIX_ARCHIVE_INGESTED
UNCOMPRESSED_PREFIX = "uncompressed_"

# Custom metadata keys (stored as x-goog-meta-<key> on the object)
META_SCANNED = "scanned"
META_ORIGINAL_FILENAME = "original-filename"
META_LANDING_PATH = "landing-path"
META_MD5_HASH = "md5-hash"          # GCS object bytes (base64 md5 from describe)
META_CONTENT_MD5 = "content-md5"  # md5 of decompressed NDJSON lines; used for dedupe
META_COUNTRIES = "countries"
META_COUNTRY_COUNT = "country-count"
META_MIN_TS = "min-ts"
META_MAX_TS = "max-ts"
META_RECORD_COUNT = "record-count"
META_UPLOAD_DATE = "upload-date"
META_RAW_MD5_SHORT = "raw-md5-short"
META_PAIRED_OBJECT = "paired-object"
META_COMPRESSION = "compression"

# NDJSON fields read during stream scan (SEO business listing dump).
# Each line is one establishment; see schema_json/seo_business_listing.json.
# Example row (abbreviated):
#   title: "Example Cafe & Bar"
#   address_info.country_code: "DE"
#   time_update: "2025-11-15T14:22:57"
#   first_seen: "2024-08-11T08:38:27"
COUNTRY_CODE_PATH = ("address_info", "country_code")
TIMESTAMP_KEYS = ("time_update", "first_seen")
TIMESTAMP_FORMATS = ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%SZ")

# Abbreviated sample used in docstrings and doctests (full dump rows are larger).
SAMPLE_ROW: dict = {
    "title": "Example Cafe & Bar",
    "address_info": {
        "borough": "Example District",
        "address": "1 Example Street",
        "city": "Example City",
        "zip": "00000",
        "region": None,
        "country_code": "DE",
    },
    "time_update": "2025-11-15T14:22:57",
    "first_seen": "2024-08-11T08:38:27",
}


@dataclass
class ScanResult:
    """Aggregates extracted from one NDJSON file (single streaming pass).

    Attributes:
        countries: Distinct country codes found in the file.
        min_ts: Earliest ``time_update`` / ``first_seen`` across rows (e.g. ``2024-08-11T08:38:27``).
        max_ts: Latest ``time_update`` / ``first_seen`` across rows (e.g. ``2025-11-15T14:22:57``).
        content_md5: Hex md5 of raw decompressed line bytes (used for dedupe and filename).
        country_count: len(countries).
        record_count: Number of non-empty NDJSON lines in the file.

    Example:
        >>> ScanResult(
        ...     countries={"DE"},
        ...     min_ts="2024-08-11T08:38:27",
        ...     max_ts="2025-11-15T14:22:57",
        ...     content_md5="a158d13fabc...",
        ...     country_count=1,
        ...     record_count=1,
        ... )
    """

    countries: set[str]
    min_ts: Optional[str]
    max_ts: Optional[str]
    content_md5: str
    country_count: int
    record_count: int


@dataclass
class IngestSummary:
    """Result of ingest_all_uploads().

    Attributes:
        promoted: GCS object names written under stg_to_load/ (empty when dry_run).
        raw_archived: GCS object names written under archive_raw/.
        errors: ``"<path>: <message>"`` strings; non-empty list causes RuntimeError.
        previews: Per-object preview dicts when ``dry_run=True``.
    """

    promoted: list[str]
    raw_archived: list[str]
    errors: list[str]
    previews: list[dict] = field(default_factory=list)
    discovered: list[str] = field(default_factory=list)


PromoteSummary = IngestSummary


@dataclass
class ArchiveSummary:
    """Result of archive_all_stg_to_load().

    Attributes:
        archived: archive_ingested/ object names created (empty when dry_run).
        previews: Per-object preview dicts when ``dry_run=True``.
        errors: ``"<path>: <message>"`` strings; non-empty list causes RuntimeError.
    """

    archived: list[str]
    previews: list[dict] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    discovered: list[str] = field(default_factory=list)


def short_hash_from_content_md5(content_md5_hex: str) -> str:
    """Return the 8-character prefix of a content md5 hex digest.

    Args:
        content_md5_hex: Full 32-char hex md5 of decompressed NDJSON bytes.

    Returns:
        First 8 characters of ``content_md5_hex``.

    Example:
        >>> short_hash_from_content_md5("a158d13f0123456789abcdef01234567")
        'a158d13f'
    """
    return content_md5_hex[:8]


def build_stem(upload_date: str, country_count: int, raw_md5_b64: str) -> str:
    """Build filename stem from upload date, country count, and vendor object md5."""
    short = short_hash_from_gcs_md5(raw_md5_b64)
    return f"{upload_date}_countries-{country_count}_{short}"


def build_archive_raw_name(stem: str, compression: str) -> str:
    """Build archive_raw basename from stem and detected compression."""
    if compression == "gzip":
        return f"{stem}.json.gz"
    return f"{stem}.json"


build_raw_archive_name = build_archive_raw_name


def build_uncompressed_name(stem: str) -> str:
    """Build stg_to_load/archive_ingested basename for uncompressed NDJSON."""
    return f"{UNCOMPRESSED_PREFIX}{stem}.json"


def build_curated_filename(upload_date: str, country_count: int, content_md5_hex: str) -> str:
    """Deprecated: legacy content-md5 gzip curated name (pre-v2 ingest)."""
    short = short_hash_from_content_md5(content_md5_hex)
    return f"{upload_date}_countries-{country_count}_{short}.json.gz"


def is_upload_candidate(name: str) -> bool:
    """Check whether a GCS object name is eligible for promote from uploads/.

    Args:
        name: Full GCS object path (e.g. ``uploads/vendor.json.gz``).

    Returns:
        True for ``uploads/*.json.gz`` and plain ``uploads/*.json`` (not other prefixes).

    Example:
        >>> is_upload_candidate("uploads/0.json.gz")
        True
        >>> is_upload_candidate("uploads/dump.json")
        True
        >>> is_upload_candidate("stg_to_load/foo.json.gz")
        False
    """
    if not name.startswith(PREFIX_UPLOADS):
        return False
    basename = name[len(PREFIX_UPLOADS) :]
    if basename.endswith(".json.gz"):
        return True
    if basename.endswith(".json") and not basename.endswith(".json.gz"):
        return True
    return False


def is_stg_to_load_candidate(name: str) -> bool:
    """Check whether a GCS object under stg_to_load/ is pending BQ load."""
    if not name.startswith(PREFIX_STG_TO_LOAD):
        return False
    basename = name[len(PREFIX_STG_TO_LOAD) :]
    return basename.startswith(UNCOMPRESSED_PREFIX) and basename.endswith(".json")


is_stg_curated_candidate = is_stg_to_load_candidate


def _file_md5_b64(path: Path) -> str:
    """Return base64 md5 digest of a local file (for stem preview on CLI scan)."""
    hasher = hashlib.md5()
    with path.open("rb") as fh:
        while True:
            chunk = fh.read(8 * 1024 * 1024)
            if not chunk:
                break
            hasher.update(chunk)
    return base64.b64encode(hasher.digest()).decode("ascii")


def _ingest_object_names(
    upload_date: str,
    scan: ScanResult,
    raw_md5_b64: str,
    compression: str,
) -> tuple[str, str, str]:
    """Return (stem, stg_object_name, archive_raw_object_name)."""
    stem = build_stem(upload_date, scan.country_count, raw_md5_b64)
    stg_name = f"{PREFIX_STG_TO_LOAD}{build_uncompressed_name(stem)}"
    raw_name = f"{PREFIX_ARCHIVE_RAW}{build_archive_raw_name(stem, compression)}"
    return stem, stg_name, raw_name


def _open_blob_raw_read(blob: storage.Blob):
    """Open a GCS blob for reading raw bytes without SDK gzip transcoding.

    Required on Composer when urllib3>=2.6 passes ``max_length`` to gzip decode
    but google-resumable-media's ``_GzipDecoder`` lacks that parameter.
    We detect compression via magic bytes and decompress with stdlib ``gzip``.
    """
    return blob.open("rb", raw_download=True)


def _read_head(blob: storage.Blob, size: int = 2) -> bytes:
    """Read the first ``size`` bytes from a GCS blob.

    Args:
        blob: GCS blob to read.
        size: Number of bytes to read from the start of the object.

    Returns:
        Raw bytes (may be shorter than ``size`` if the object is smaller).

    Example:
        >>> # head[:2] == b"\\x1f\\x8b" indicates gzip content
    """
    with _open_blob_raw_read(blob) as fh:
        return fh.read(size)


def detect_compression_bytes(head: bytes) -> str:
    """Detect gzip from magic bytes.

    Args:
        head: First bytes of the file (at least 2 when available).

    Returns:
        ``"gzip"`` if bytes start with ``\\x1f\\x8b``, else ``"none"``.

    Example:
        >>> detect_compression_bytes(b"\\x1f\\x8b")
        'gzip'
        >>> detect_compression_bytes(b'{"title"')
        'none'
    """
    if len(head) >= 2 and head[:2] == b"\x1f\x8b":
        return "gzip"
    return "none"


def detect_compression_path(path: Path) -> str:
    """Detect compression for a local file via magic bytes.

    Args:
        path: Local filesystem path.

    Returns:
        ``"gzip"`` or ``"none"``.
    """
    with path.open("rb") as fh:
        return detect_compression_bytes(fh.read(2))


def detect_compression(blob: storage.Blob) -> str:
    """Detect whether blob content is gzip-compressed from magic bytes.

    Args:
        blob: GCS blob under uploads/ or elsewhere.

    Returns:
        ``"gzip"`` if bytes start with ``\\x1f\\x8b``, else ``"none"`` (plain NDJSON).

    Example:
        >>> detect_compression(blob)  # uploads/misnamed.json with gzip body
        'gzip'
        >>> detect_compression(blob)  # uploads/plain.json NDJSON
        'none'
    """
    return detect_compression_bytes(_read_head(blob))


def _iter_decompressed_lines_from_stream(
    stream: BinaryIO, compression: str
) -> Iterator[bytes]:
    """Iterate non-empty line bytes from a gzip or plain NDJSON stream.

    Args:
        stream: Readable binary stream positioned at file start.
        compression: ``"gzip"`` or ``"none"``.

    Yields:
        Raw line bytes (including trailing newline) for each non-blank line.
    """
    decompressed: BinaryIO = gzip.GzipFile(fileobj=stream) if compression == "gzip" else stream
    for line in decompressed:
        if line.strip():
            yield line


def _iter_decompressed_lines(blob: storage.Blob, compression: str) -> Iterator[bytes]:
    """Iterate non-empty line bytes from gzip or plain NDJSON.

    Args:
        blob: GCS blob to stream.
        compression: ``"gzip"`` or ``"none"`` (from :func:`detect_compression`).

    Yields:
        Raw line bytes (including trailing newline) for each non-blank line.

    Example:
        >>> list(_iter_decompressed_lines(blob, "gzip"))[:1]
        [b'{"title": "Example Cafe & Bar", "address_info": {"country_code": "DE"}, ...}\\n']
    """
    with _open_blob_raw_read(blob) as raw:
        yield from _iter_decompressed_lines_from_stream(raw, compression)


def _extract_country(row: dict) -> Optional[str]:
    """Extract ISO country code from one SEO business listing row.

    Reads ``address_info.country_code`` (primary path in vendor dumps). Falls back
    to a top-level ``country_code`` if present.

    Args:
        row: Parsed JSON object for one NDJSON line.

    Returns:
        Country code string (e.g. ``"DE"``), or None if not present.

    Example:
        >>> _extract_country(SAMPLE_ROW)
        'DE'
        >>> _extract_country({"address_info": {"country_code": "DE"}})
        'DE'
        >>> _extract_country({"title": "Example Cafe & Bar"})
        None
    """
    address_info = row.get(COUNTRY_CODE_PATH[0])
    if isinstance(address_info, dict):
        code = address_info.get(COUNTRY_CODE_PATH[1])
        if code:
            return str(code)
    code = row.get("country_code")
    return str(code) if code else None


def _parse_row_timestamp(value: str) -> Optional[datetime]:
    """Parse vendor timestamp strings such as ``2025-11-15T14:22:57``."""
    value = value.strip()
    for fmt in TIMESTAMP_FORMATS:
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    return None


def _extract_timestamps(row: dict) -> list[str]:
    """Collect ``time_update`` and ``first_seen`` from one business listing row.

    Args:
        row: Parsed JSON object for one NDJSON line.

    Returns:
        Non-empty timestamp strings present on the row.

    Example:
        >>> _extract_timestamps(SAMPLE_ROW)
        ['2025-11-15T14:22:57', '2024-08-11T08:38:27']
    """
    values = []
    for key in TIMESTAMP_KEYS:
        val = row.get(key)
        if val is not None and val != "":
            values.append(str(val))
    return values


def _max_timestamp(existing: Optional[str], candidates: Iterable[str]) -> Optional[str]:
    """Return the latest vendor timestamp string on a row.

    Compares parsed ``time_update`` / ``first_seen`` values and returns the
    original string of the latest value (e.g. ``2025-11-15T14:22:57``).

    Args:
        existing: Current best timestamp string, or None.
        candidates: New timestamp strings from one row.

    Returns:
        Latest timestamp string, or None if no parseable values exist.

    Example:
        >>> _max_timestamp("2024-08-11T08:38:27", ["2025-11-15T14:22:57"])
        '2025-11-15T14:22:57'
        >>> _max_timestamp(None, _extract_timestamps(SAMPLE_ROW))
        '2025-11-15T14:22:57'
    """
    best_dt: Optional[datetime] = _parse_row_timestamp(existing) if existing else None
    best_str = existing if best_dt else None
    for candidate in candidates:
        parsed = _parse_row_timestamp(candidate)
        if parsed is None:
            continue
        if best_dt is None or parsed > best_dt:
            best_dt = parsed
            best_str = candidate
    return best_str


def _min_timestamp(existing: Optional[str], candidates: Iterable[str]) -> Optional[str]:
    """Return the earliest vendor timestamp string on a row.

    Compares parsed ``time_update`` / ``first_seen`` values and returns the
    original string of the earliest value (e.g. ``2024-08-11T08:38:27``).

    Args:
        existing: Current best timestamp string, or None.
        candidates: New timestamp strings from one row.

    Returns:
        Earliest timestamp string, or None if no parseable values exist.

    Example:
        >>> _min_timestamp("2025-11-15T14:22:57", ["2024-08-11T08:38:27"])
        '2024-08-11T08:38:27'
        >>> _min_timestamp(None, _extract_timestamps(SAMPLE_ROW))
        '2024-08-11T08:38:27'
    """
    best_dt: Optional[datetime] = _parse_row_timestamp(existing) if existing else None
    best_str = existing if best_dt else None
    for candidate in candidates:
        parsed = _parse_row_timestamp(candidate)
        if parsed is None:
            continue
        if best_dt is None or parsed < best_dt:
            best_dt = parsed
            best_str = candidate
    return best_str


def scan_stream(stream: BinaryIO, compression: str, *, source: str) -> ScanResult:
    """Stream-scan NDJSON for countries, timestamps, record count, and content md5.

    Args:
        stream: Readable binary stream (plain or gzip-wrapped NDJSON).
        compression: ``"gzip"`` or ``"none"``.
        source: Label for error messages (file path or GCS object name).

    Returns:
        :class:`ScanResult` with aggregates over all non-empty lines.

    Raises:
        ValueError: If a line is not valid JSON.
    """
    hasher = hashlib.md5()
    countries: set[str] = set()
    min_ts: Optional[str] = None
    max_ts: Optional[str] = None
    record_count = 0

    for line in _iter_decompressed_lines_from_stream(stream, compression):
        record_count += 1
        hasher.update(line)
        try:
            row = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ValueError(f"Invalid JSON in {source}: {exc}") from exc
        country = _extract_country(row)
        if country:
            countries.add(country)
        row_timestamps = _extract_timestamps(row)
        min_ts = _min_timestamp(min_ts, row_timestamps)
        max_ts = _max_timestamp(max_ts, row_timestamps)

    return ScanResult(
        countries=countries,
        min_ts=min_ts,
        max_ts=max_ts,
        content_md5=hasher.hexdigest(),
        country_count=len(countries),
        record_count=record_count,
    )


def scan_path(path: str | Path) -> ScanResult:
    """Stream-scan a local NDJSON file (gzip or plain).

    Args:
        path: Local filesystem path.

    Returns:
        :class:`ScanResult` with aggregates over all non-empty lines.

    Raises:
        FileNotFoundError: If ``path`` does not exist.
        ValueError: If a line is not valid JSON.
    """
    resolved = Path(path).resolve()
    if not resolved.is_file():
        raise FileNotFoundError(resolved)
    compression = detect_compression_path(resolved)
    with resolved.open("rb") as fh:
        return scan_stream(fh, compression, source=str(resolved))


def _upload_date_from_path(path: Path) -> str:
    """Resolve ``YYYYMMDD`` upload date from a local file mtime (UTC)."""
    mtime = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
    return mtime.strftime("%Y%m%d")


def format_local_scan_report(
    path: str | Path,
    scan: ScanResult,
    *,
    compression: str,
    upload_date: Optional[str] = None,
    raw_md5_b64: Optional[str] = None,
) -> dict:
    """Build JSON-serializable scan report for a local file.

    Args:
        path: Local file path that was scanned.
        scan: Aggregates from :func:`scan_path`.
        compression: ``"gzip"`` or ``"none"`` from magic-byte detection.
        upload_date: Optional ``YYYYMMDD`` override; defaults to file mtime UTC.
        raw_md5_b64: Optional base64 md5 of file bytes for stem preview (computed if omitted).

    Returns:
        Dict with ``source``, ``compression``, v2 object paths, and ``metadata``.
    """
    resolved = Path(path).resolve()
    upload_date = upload_date or _upload_date_from_path(resolved)
    raw_md5_b64 = raw_md5_b64 or _file_md5_b64(resolved)
    raw_md5_short = short_hash_from_gcs_md5(raw_md5_b64)
    stem = build_stem(upload_date, scan.country_count, raw_md5_b64)
    stg_name = f"{PREFIX_STG_TO_LOAD}{build_uncompressed_name(stem)}"
    raw_name = f"{PREFIX_ARCHIVE_RAW}{build_archive_raw_name(stem, compression)}"
    metadata = _build_metadata(
        original_filename=resolved.name,
        landing_path=str(resolved),
        gcs_md5_b64=raw_md5_b64,
        scan=scan,
        upload_date=upload_date,
        raw_md5_short=raw_md5_short,
        compression=compression,
        paired_object=raw_name,
    )
    return {
        "source": str(resolved),
        "compression": compression,
        "stem": stem,
        "stg_to_load_object": stg_name,
        "archive_raw_object": raw_name,
        "curated_filename": build_uncompressed_name(stem),
        "metadata": metadata,
    }


GS_URI_PREFIX = "gs://"


def is_gs_uri(path: str) -> bool:
    """Return True if ``path`` is a ``gs://bucket/object`` URI."""
    return path.startswith(GS_URI_PREFIX)


def parse_gs_uri(uri: str) -> tuple[str, str]:
    """Parse ``gs://bucket/object/path`` into bucket name and object name.

    Args:
        uri: GCS URI.

    Returns:
        ``(bucket_name, blob_name)``.

    Raises:
        ValueError: If ``uri`` is not a valid GCS object URI.
    """
    if not is_gs_uri(uri):
        raise ValueError(f"Not a GCS URI: {uri}")
    without_scheme = uri[len(GS_URI_PREFIX) :]
    bucket, sep, blob_name = without_scheme.partition("/")
    if not sep or not bucket or not blob_name:
        raise ValueError(f"GCS URI must be gs://bucket/object: {uri}")
    return bucket, blob_name


def scan_gs_uri(
    uri: str,
    *,
    client: storage.Client | None = None,
) -> tuple[storage.Blob, ScanResult, str]:
    """Stream-scan a GCS object (gzip or plain NDJSON).

    Args:
        uri: ``gs://bucket/object`` URI.
        client: Optional preconfigured storage client (uses ADC when omitted).

    Returns:
        ``(blob, scan_result, compression)`` where compression is ``gzip`` or ``none``.

    Raises:
        FileNotFoundError: If the object does not exist.
        ValueError: If ``uri`` is invalid or a line is not valid JSON.
    """
    bucket_name, blob_name = parse_gs_uri(uri)
    client = client or storage.Client()
    blob = client.bucket(bucket_name).blob(blob_name)
    if not blob.exists(client=client):
        raise FileNotFoundError(uri)
    blob.reload(client=client)
    compression = detect_compression(blob)
    scan = scan_blob(blob)
    return blob, scan, compression


def format_gcs_scan_report(
    uri: str,
    blob: storage.Blob,
    scan: ScanResult,
    *,
    compression: str,
    upload_date: Optional[str] = None,
) -> dict:
    """Build JSON-serializable scan report for a GCS object.

    Args:
        uri: Original ``gs://`` URI passed to the CLI.
        blob: Scanned GCS blob (reloaded).
        scan: Aggregates from :func:`scan_blob`.
        compression: ``"gzip"`` or ``"none"``.
        upload_date: Optional ``YYYYMMDD`` override; defaults to blob ``time_created`` UTC.

    Returns:
        Dict with ``source``, ``compression``, ``curated_filename``, and ``metadata``.
    """
    existing_meta = {str(k): str(v) for k, v in (blob.metadata or {}).items()}
    upload_date = upload_date or _upload_date_from_blob(blob, existing_meta)
    raw_md5_b64 = blob.md5_hash or ""
    raw_md5_short = short_hash_from_gcs_md5(raw_md5_b64) if raw_md5_b64 else ""
    stem = build_stem(upload_date, scan.country_count, raw_md5_b64) if raw_md5_b64 else ""
    stg_name = f"{PREFIX_STG_TO_LOAD}{build_uncompressed_name(stem)}" if stem else ""
    raw_name = (
        f"{PREFIX_ARCHIVE_RAW}{build_archive_raw_name(stem, compression)}" if stem else ""
    )
    metadata = _build_metadata(
        original_filename=blob.name.split("/")[-1],
        landing_path=blob.name,
        gcs_md5_b64=blob.md5_hash,
        scan=scan,
        upload_date=upload_date,
        raw_md5_short=raw_md5_short,
        compression=compression,
        paired_object=raw_name,
    )
    return {
        "source": uri,
        "compression": compression,
        "stem": stem,
        "stg_to_load_object": stg_name,
        "archive_raw_object": raw_name,
        "curated_filename": build_uncompressed_name(stem) if stem else "",
        "metadata": metadata,
    }


def scan_blob(blob: storage.Blob) -> ScanResult:
    """Stream-scan a blob once for countries, timestamps, record count, and content md5.

    Args:
        blob: GCS blob (gzip or plain NDJSON).

    Returns:
        :class:`ScanResult` with aggregates over all non-empty lines.

    Raises:
        ValueError: If a line is not valid JSON.

    Example:
        >>> result = scan_blob(bucket.blob("uploads/0.json.gz"))
        >>> result.countries
        {'DE'}
        >>> result.max_ts
        '2025-11-15T14:22:57'
        >>> result.min_ts
        '2024-08-11T08:38:27'
        >>> result.record_count
        1
    """
    compression = detect_compression(blob)
    with _open_blob_raw_read(blob) as raw:
        return scan_stream(raw, compression, source=blob.name)


def _blob_custom_metadata(blob: storage.Blob) -> dict[str, str]:
    """Reload blob and return custom metadata as string key-value pairs.

    Args:
        blob: GCS blob (mutated in place via ``reload()``).

    Returns:
        Metadata dict (empty if none). Keys omit the ``x-goog-meta-`` prefix.

    Example:
        >>> _blob_custom_metadata(blob)
        {'scanned': 'true', 'content-md5': 'a158d13f...', 'country-count': '12'}
    """
    blob.reload()
    return {str(k): str(v) for k, v in (blob.metadata or {}).items()}


def _metadata_complete(meta: dict[str, str]) -> bool:
    """Check whether metadata is sufficient to skip a content stream scan.

    Args:
        meta: Custom metadata dict from :func:`_blob_custom_metadata`.

    Returns:
        True if ``scanned=true`` and all required scan fields are non-empty.

    Example:
        >>> _metadata_complete({"scanned": "true", "content-md5": "abc", ...})
        True
        >>> _metadata_complete({"scanned": "false"})
        False
    """
    required = (
        META_SCANNED,
        META_COUNTRIES,
        META_COUNTRY_COUNT,
        META_MIN_TS,
        META_MAX_TS,
        META_RECORD_COUNT,
        META_CONTENT_MD5,
        META_UPLOAD_DATE,
    )
    return meta.get(META_SCANNED) == "true" and all(meta.get(k) for k in required)


def _scan_result_from_metadata(meta: dict[str, str]) -> ScanResult:
    """Rebuild :class:`ScanResult` from stored custom metadata (no file read).

    Args:
        meta: Complete scan metadata (see :func:`_metadata_complete`).

    Returns:
        :class:`ScanResult` equivalent to a prior :func:`scan_blob` call.

    Example:
        >>> _scan_result_from_metadata({
        ...     "countries": "DE", "country-count": "1",
        ...     "min-ts": "2024-08-11T08:38:27",
        ...     "max-ts": "2025-11-15T14:22:57", "record-count": "1",
        ...     "content-md5": "a158d13f...",
        ... })
        ScanResult(countries={'DE'}, min_ts='2024-08-11T08:38:27', max_ts='2025-11-15T14:22:57', ...)
    """
    countries = {c for c in meta.get(META_COUNTRIES, "").split(",") if c}
    return ScanResult(
        countries=countries,
        min_ts=meta.get(META_MIN_TS) or None,
        max_ts=meta.get(META_MAX_TS) or None,
        content_md5=meta[META_CONTENT_MD5],
        country_count=int(meta[META_COUNTRY_COUNT]),
        record_count=int(meta[META_RECORD_COUNT]),
    )


def _build_metadata(
    *,
    original_filename: str,
    landing_path: str,
    gcs_md5_b64: Optional[str],
    scan: ScanResult,
    upload_date: str,
    raw_md5_short: str = "",
    compression: str = "",
    paired_object: str = "",
) -> dict[str, str]:
    """Build custom metadata dict to attach to a curated/archive/raw object."""
    meta = {
        META_SCANNED: "true",
        META_ORIGINAL_FILENAME: original_filename,
        META_LANDING_PATH: landing_path,
        META_CONTENT_MD5: scan.content_md5,
        META_COUNTRIES: ",".join(sorted(scan.countries)),
        META_COUNTRY_COUNT: str(scan.country_count),
        META_MIN_TS: scan.min_ts or "",
        META_MAX_TS: scan.max_ts or "",
        META_RECORD_COUNT: str(scan.record_count),
        META_UPLOAD_DATE: upload_date,
    }
    if gcs_md5_b64:
        meta[META_MD5_HASH] = gcs_md5_b64
    if raw_md5_short:
        meta[META_RAW_MD5_SHORT] = raw_md5_short
    if compression:
        meta[META_COMPRESSION] = compression
    if paired_object:
        meta[META_PAIRED_OBJECT] = paired_object
    return meta


def _upload_date_from_blob(blob: storage.Blob, meta: dict[str, str]) -> str:
    """Resolve ``YYYYMMDD`` upload date for the curated filename.

    Args:
        blob: GCS blob (uses ``time_created`` when metadata has no upload-date).
        meta: Existing custom metadata (may already contain upload-date).

    Returns:
        Date string ``YYYYMMDD`` in UTC.

    Example:
        >>> _upload_date_from_blob(blob, {"upload-date": "20260407"})
        '20260407'
        >>> _upload_date_from_blob(blob, {})  # uses blob.time_created
        '20260407'
    """
    if meta.get(META_UPLOAD_DATE):
        return meta[META_UPLOAD_DATE]
    created = blob.time_created
    if created is None:
        created = datetime.now(timezone.utc)
    if created.tzinfo is None:
        created = created.replace(tzinfo=timezone.utc)
    return created.astimezone(timezone.utc).strftime("%Y%m%d")


def _collect_content_md5_index(bucket: storage.Bucket, prefixes: Iterable[str]) -> Set[str]:
    """Collect all content-md5 values under given prefixes for dedupe checks.

    Args:
        bucket: GCS bucket handle.
        prefixes: Object prefixes to scan (e.g. ``stg_to_load/``, ``archive_ingested/``).

    Returns:
        Set of hex content-md5 strings found on objects with that metadata set.

    Example:
        >>> _collect_content_md5_index(bucket, ("stg_to_load/", "archive_ingested/"))
        {'a158d13f0123456789abcdef01234567', 'b2c4d5e60123456789abcdef0123456'}
    """
    seen: Set[str] = set()
    for prefix in prefixes:
        for blob in bucket.list_blobs(prefix=prefix):
            if blob.name.endswith("/"):
                continue
            meta = _blob_custom_metadata(blob)
            content_md5 = meta.get(META_CONTENT_MD5)
            if content_md5:
                seen.add(content_md5)
    return seen


def _apply_metadata_gzip(blob: storage.Blob, metadata: dict[str, str]) -> None:
    """Patch custom metadata and gzip content headers on a GCS object."""
    blob.metadata = metadata
    blob.content_type = "application/json"
    blob.content_encoding = "gzip"
    blob.patch()


def _apply_metadata_plain(blob: storage.Blob, metadata: dict[str, str]) -> None:
    """Patch custom metadata and plain NDJSON content headers on a GCS object."""
    blob.metadata = metadata
    blob.content_type = "application/json"
    blob.content_encoding = None
    blob.patch()


def _apply_metadata(blob: storage.Blob, metadata: dict[str, str]) -> None:
    """Patch gzip metadata (legacy alias)."""
    _apply_metadata_gzip(blob, metadata)


def _stream_scan_to_uncompressed_blob(
    source_blob: storage.Blob,
    dest_blob: storage.Blob,
    compression: str,
) -> ScanResult:
    """Stream-scan NDJSON and write uncompressed lines to dest_blob."""
    with _open_blob_raw_read(source_blob) as raw, dest_blob.open("wb") as dst:
        return _scan_stream_write(raw, dst, compression, source=source_blob.name)


def _scan_stream_write(
    stream: BinaryIO,
    dest: BinaryIO,
    compression: str,
    *,
    source: str,
) -> ScanResult:
    """Scan NDJSON from stream, write uncompressed lines to dest, return aggregates."""
    hasher = hashlib.md5()
    countries: set[str] = set()
    min_ts: Optional[str] = None
    max_ts: Optional[str] = None
    record_count = 0

    for line in _iter_decompressed_lines_from_stream(stream, compression):
        record_count += 1
        hasher.update(line)
        dest.write(line)
        try:
            row = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ValueError(f"Invalid JSON in {source}: {exc}") from exc
        country = _extract_country(row)
        if country:
            countries.add(country)
        row_timestamps = _extract_timestamps(row)
        min_ts = _min_timestamp(min_ts, row_timestamps)
        max_ts = _max_timestamp(max_ts, row_timestamps)

    return ScanResult(
        countries=countries,
        min_ts=min_ts,
        max_ts=max_ts,
        content_md5=hasher.hexdigest(),
        country_count=len(countries),
        record_count=record_count,
    )


def _storage_client(project: Optional[str] = None) -> storage.Client:
    """Build a GCS client (optional explicit project for ADC)."""
    return storage.Client(project=project) if project else storage.Client()


def _setup_cli_logging(level: int = logging.INFO) -> None:
    """Configure stderr logging for CLI runs (default INFO)."""
    logging.basicConfig(
        level=level,
        format="%(levelname)s: %(message)s",
        stream=sys.stderr,
        force=True,
    )


def _add_log_level_arg(parser: argparse.ArgumentParser) -> None:
    """Add ``--log-level`` to the root CLI parser."""
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Log verbosity (default: INFO)",
    )


def _metadata_with_upload_date(
    meta: dict[str, str],
    upload_date_override: Optional[str],
) -> dict[str, str]:
    """Return metadata copy with ``upload-date`` overridden when requested."""
    if not upload_date_override:
        return meta
    patched = dict(meta)
    patched[META_UPLOAD_DATE] = upload_date_override
    return patched


def ingest_all_uploads(
    bucket_name: str,
    gcp_conn_id: str | None = None,
    *,
    upload_date_override: str | None = None,
    dry_run: bool = False,
    project: str | None = None,
) -> IngestSummary:
    """Ingest every eligible object under uploads/ to stg_to_load/ and archive_raw/.

    Stream-decompress (or stream-copy plain NDJSON) into ``stg_to_load/uncompressed_*.json``,
    server-side copy vendor bytes to ``archive_raw/``, then delete the uploads/ source.
    Always processes all uploads/ candidates (no content-md5 skip).
    """
    del gcp_conn_id
    client = _storage_client(project)
    bucket = client.bucket(bucket_name)
    summary = IngestSummary(promoted=[], raw_archived=[], errors=[])

    mode = "dry-run" if dry_run else "apply"
    logger.info(
        "Ingest %s started: gs://%s/%s%s",
        mode,
        bucket_name,
        PREFIX_UPLOADS,
        f" upload-date={upload_date_override}" if upload_date_override else "",
    )

    upload_blobs = [
        blob
        for blob in bucket.list_blobs(prefix=PREFIX_UPLOADS)
        if is_upload_candidate(blob.name)
    ]
    summary.discovered = [blob.name for blob in upload_blobs]

    logger.info("Found %d object(s) under %s", len(summary.discovered), PREFIX_UPLOADS)
    for name in summary.discovered:
        logger.info("  %s", name)

    for blob in upload_blobs:
        try:
            logger.debug("Processing %s", blob.name)
            blob.reload()
            if not blob.md5_hash:
                raise ValueError(f"Missing md5_hash on {blob.name}")

            original_filename = blob.name.split("/")[-1]
            landing_path = blob.name
            meta = _blob_custom_metadata(blob)
            compression = detect_compression(blob)
            raw_md5_b64 = blob.md5_hash
            raw_md5_short = short_hash_from_gcs_md5(raw_md5_b64)

            upload_date = (
                upload_date_override
                or meta.get(META_UPLOAD_DATE)
                or _upload_date_from_blob(blob, meta)
            )

            if dry_run:
                scan = scan_blob(blob)
                stem, stg_name, raw_name = _ingest_object_names(
                    upload_date, scan, raw_md5_b64, compression
                )
                stg_meta = _build_metadata(
                    original_filename=original_filename,
                    landing_path=landing_path,
                    gcs_md5_b64=None,
                    scan=scan,
                    upload_date=upload_date,
                    raw_md5_short=raw_md5_short,
                    compression=compression,
                    paired_object=raw_name,
                )
                raw_meta = _build_metadata(
                    original_filename=original_filename,
                    landing_path=landing_path,
                    gcs_md5_b64=raw_md5_b64,
                    scan=scan,
                    upload_date=upload_date,
                    raw_md5_short=raw_md5_short,
                    compression=compression,
                    paired_object=stg_name,
                )
                logger.info(
                    "[dry-run] Would ingest %s → %s + %s",
                    landing_path,
                    stg_name,
                    raw_name,
                )
                summary.previews.append(
                    {
                        "action": "ingest",
                        "source": landing_path,
                        "dest_stg": stg_name,
                        "dest_raw": raw_name,
                        "compression": compression,
                        "stem": stem,
                        "metadata_stg": stg_meta,
                        "metadata_raw": raw_meta,
                    }
                )
                continue

            tmp_name = f"{PREFIX_STG_TO_LOAD}.tmp/{raw_md5_short}.json"
            tmp_blob = bucket.blob(tmp_name)
            scan = _stream_scan_to_uncompressed_blob(blob, tmp_blob, compression)
            stem, stg_name, raw_name = _ingest_object_names(
                upload_date, scan, raw_md5_b64, compression
            )

            stg_meta = _build_metadata(
                original_filename=original_filename,
                landing_path=landing_path,
                gcs_md5_b64=None,
                scan=scan,
                upload_date=upload_date,
                raw_md5_short=raw_md5_short,
                compression=compression,
                paired_object=raw_name,
            )
            raw_meta = _build_metadata(
                original_filename=original_filename,
                landing_path=landing_path,
                gcs_md5_b64=raw_md5_b64,
                scan=scan,
                upload_date=upload_date,
                raw_md5_short=raw_md5_short,
                compression=compression,
                paired_object=stg_name,
            )

            if tmp_blob.name != stg_name:
                bucket.copy_blob(tmp_blob, bucket, stg_name)
                tmp_blob.delete()
                dest_stg = bucket.blob(stg_name)
            else:
                dest_stg = tmp_blob

            dest_stg.reload()
            if dest_stg.md5_hash:
                stg_meta = dict(stg_meta)
                stg_meta[META_MD5_HASH] = dest_stg.md5_hash
            _apply_metadata_plain(dest_stg, stg_meta)

            bucket.copy_blob(blob, bucket, raw_name)
            dest_raw = bucket.blob(raw_name)
            dest_raw.reload()
            raw_meta = dict(raw_meta)
            raw_meta[META_MD5_HASH] = dest_raw.md5_hash or raw_md5_b64
            if compression == "gzip":
                _apply_metadata_gzip(dest_raw, raw_meta)
            else:
                _apply_metadata_plain(dest_raw, raw_meta)

            blob.delete()
            summary.promoted.append(stg_name)
            summary.raw_archived.append(raw_name)
            logger.info("Ingested %s → %s + %s", landing_path, stg_name, raw_name)
        except Exception as exc:  # noqa: BLE001
            logger.error("Failed %s: %s", blob.name, exc)
            logger.debug("Ingest error detail for %s", blob.name, exc_info=True)
            summary.errors.append(f"{blob.name}: {exc}")

    logger.info(
        "Ingest %s finished: %d stg, %d raw, %d preview(s), %d error(s)",
        mode,
        len(summary.promoted),
        len(summary.raw_archived),
        len(summary.previews),
        len(summary.errors),
    )

    if summary.errors:
        raise RuntimeError("Ingest failures: " + "; ".join(summary.errors))

    return summary


def promote_all_uploads(
    bucket_name: str,
    gcp_conn_id: str | None = None,
    *,
    upload_date_override: str | None = None,
    dry_run: bool = False,
    project: str | None = None,
) -> IngestSummary:
    """Deprecated alias for :func:`ingest_all_uploads`."""
    return ingest_all_uploads(
        bucket_name,
        gcp_conn_id,
        upload_date_override=upload_date_override,
        dry_run=dry_run,
        project=project,
    )


def archive_all_stg_to_load(
    bucket_name: str,
    gcp_conn_id: str | None = None,
    *,
    upload_date_override: str | None = None,
    dry_run: bool = False,
    project: str | None = None,
) -> ArchiveSummary:
    """Move stg_to_load/uncompressed_*.json objects to flat archive_ingested/ after BigQuery load.

    Args:
        bucket_name: Ingest bucket (e.g. ``seo-listings-ingest``).
        gcp_conn_id: Reserved for Airflow hook wiring; currently uses ADC.
        upload_date_override: Optional ``YYYYMMDD`` patched onto archive metadata on write.
        dry_run: When True, compute previews only (no GCS mutations).
        project: Optional GCP project for the storage client.

    Returns:
        :class:`ArchiveSummary` with archived paths, previews, and errors.

    Raises:
        RuntimeError: If any file failed to archive (see ``summary.errors``).

    Example:
        >>> archive_all_stg_to_load("seo-listings-ingest").archived
        ['archive_ingested/uncompressed_20260407_countries-12_a158d13f.json']
    """
    del gcp_conn_id
    client = _storage_client(project)
    bucket = client.bucket(bucket_name)
    summary = ArchiveSummary(archived=[])

    mode = "dry-run" if dry_run else "apply"
    logger.info(
        "Archive %s started: gs://%s/%s%s",
        mode,
        bucket_name,
        PREFIX_STG_TO_LOAD,
        f" upload-date={upload_date_override}" if upload_date_override else "",
    )

    archive_content_md5 = _collect_content_md5_index(bucket, (PREFIX_ARCHIVE_INGESTED,))
    logger.debug(
        "Dedupe index: %d content-md5 value(s) in %s",
        len(archive_content_md5),
        PREFIX_ARCHIVE_INGESTED,
    )

    stg_blobs = [
        blob
        for blob in bucket.list_blobs(prefix=PREFIX_STG_TO_LOAD)
        if is_stg_to_load_candidate(blob.name)
    ]
    summary.discovered = [blob.name for blob in stg_blobs]

    logger.info("Found %d object(s) under %s", len(summary.discovered), PREFIX_STG_TO_LOAD)
    for name in summary.discovered:
        logger.info("  %s", name)

    for blob in stg_blobs:
        try:
            logger.debug("Processing %s", blob.name)
            meta = _blob_custom_metadata(blob)
            content_md5 = meta.get(META_CONTENT_MD5)
            basename = blob.name.split("/")[-1]
            dest_name = f"{PREFIX_ARCHIVE_INGESTED}{basename}"
            archive_meta = _metadata_with_upload_date(meta, upload_date_override)

            if content_md5 and content_md5 in archive_content_md5:
                logger.info("Skipped %s (already archived)", blob.name)
                if dry_run:
                    summary.previews.append(
                        {
                            "action": "dedupe_delete",
                            "source": blob.name,
                            "dest": None,
                            "metadata": archive_meta,
                        }
                    )
                else:
                    blob.delete()
                    summary.archived.append(f"dedupe-deleted:{blob.name}")
                continue

            if dry_run:
                logger.info("[dry-run] Would archive %s → %s", blob.name, dest_name)
                summary.previews.append(
                    {
                        "action": "archive",
                        "source": blob.name,
                        "dest": dest_name,
                        "metadata": archive_meta,
                    }
                )
                continue

            bucket.copy_blob(blob, bucket, dest_name)
            dest = bucket.blob(dest_name)
            if archive_meta:
                _apply_metadata_plain(dest, archive_meta)
            blob.delete()
            if content_md5:
                archive_content_md5.add(content_md5)
            summary.archived.append(dest_name)
            logger.info("Archived %s → %s", blob.name, dest_name)
        except Exception as exc:  # noqa: BLE001 - collect per-file errors for Airflow log
            logger.error("Failed %s: %s", blob.name, exc)
            logger.debug("Archive error detail for %s", blob.name, exc_info=True)
            summary.errors.append(f"{blob.name}: {exc}")

    logger.info(
        "Archive %s finished: %d archived, %d preview(s), %d error(s)",
        mode,
        len(summary.archived),
        len(summary.previews),
        len(summary.errors),
    )

    if summary.errors:
        raise RuntimeError(
            "Archive failures: " + "; ".join(summary.errors)
        )

    return summary


def archive_all_stg_curated(
    bucket_name: str,
    gcp_conn_id: str | None = None,
    *,
    upload_date_override: str | None = None,
    dry_run: bool = False,
    project: str | None = None,
) -> ArchiveSummary:
    """Deprecated alias for :func:`archive_all_stg_to_load`."""
    return archive_all_stg_to_load(
        bucket_name,
        gcp_conn_id,
        upload_date_override=upload_date_override,
        dry_run=dry_run,
        project=project,
    )


def short_hash_from_gcs_md5(md5_hash_b64: str) -> str:
    """Convert GCS base64 object md5 to 8-character hex prefix.

    Args:
        md5_hash_b64: Value from ``gcloud storage objects describe`` (field ``md5_hash``).

    Returns:
        First 8 hex chars of the decoded md5 digest.

    Example:
        >>> short_hash_from_gcs_md5("oVjRP8J83/+KcVBmBeOWsQ==")
        'a158d13f'
    """
    digest = base64.b64decode(md5_hash_b64)
    return digest.hex()[:8]


def _scan_one_input(
    file_path: str,
    *,
    upload_date: Optional[str],
    gcs_client: storage.Client | None,
) -> dict:
    """Scan one local path or ``gs://`` URI and return a report dict."""
    if is_gs_uri(file_path):
        blob, scan, compression = scan_gs_uri(file_path, client=gcs_client)
        return format_gcs_scan_report(
            file_path,
            blob,
            scan,
            compression=compression,
            upload_date=upload_date,
        )

    resolved = Path(file_path).resolve()
    if not resolved.is_file():
        raise FileNotFoundError(resolved)
    suffix = resolved.suffix.lower()
    if suffix not in {".json", ".gz"} and not resolved.name.endswith(".json.gz"):
        logger.warning("Unexpected extension for %s; scanning via magic bytes", resolved)
    compression = detect_compression_path(resolved)
    scan = scan_path(resolved)
    return format_local_scan_report(
        resolved,
        scan,
        compression=compression,
        upload_date=upload_date,
    )


def _print_cli_json(payload: dict) -> None:
    """Print a CLI result dict as pretty JSON."""
    print(json.dumps(payload, indent=2, sort_keys=True))


def _add_ingest_args(parser: argparse.ArgumentParser) -> None:
    """Add shared bucket/GCS flags for promote and archive subcommands."""
    parser.add_argument(
        "--bucket",
        required=True,
        help="Ingest bucket name (e.g. seo-listings-ingest)",
    )
    parser.add_argument(
        "--upload-date",
        metavar="YYYYMMDD",
        help="Override upload-date metadata (promote: also affects curated filename)",
    )
    parser.add_argument(
        "--project",
        metavar="GCP_PROJECT",
        help="GCP project for GCS client (default: ADC project)",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Mutate GCS (default: dry-run preview JSON only)",
    )


def _cli_ingest(args: argparse.Namespace, *, command: str = "ingest") -> int:
    """Run ingest (dry-run by default) and print JSON summary."""
    dry_run = not args.apply
    try:
        summary = ingest_all_uploads(
            args.bucket,
            upload_date_override=args.upload_date,
            dry_run=dry_run,
            project=args.project,
        )
    except RuntimeError as exc:
        logger.error("%s", exc)
        return 1
    except Exception as exc:  # noqa: BLE001 - surface GCS/auth errors to CLI user
        logger.error("%s", exc)
        return 1

    _print_cli_json(
        {
            "command": command,
            "dry_run": dry_run,
            "bucket": args.bucket,
            "upload_date_override": args.upload_date,
            **asdict(summary),
        }
    )
    return 0


def _cli_promote(args: argparse.Namespace) -> int:
    """Deprecated alias for ingest subcommand."""
    return _cli_ingest(args, command="promote")


def _cli_archive(args: argparse.Namespace) -> int:
    """Run archive (dry-run by default) and print JSON summary."""
    dry_run = not args.apply
    try:
        summary = archive_all_stg_to_load(
            args.bucket,
            upload_date_override=args.upload_date,
            dry_run=dry_run,
            project=args.project,
        )
    except RuntimeError as exc:
        logger.error("%s", exc)
        return 1
    except Exception as exc:  # noqa: BLE001 - surface GCS/auth errors to CLI user
        logger.error("%s", exc)
        return 1

    _print_cli_json(
        {
            "command": "archive",
            "dry_run": dry_run,
            "bucket": args.bucket,
            "upload_date_override": args.upload_date,
            **asdict(summary),
        }
    )
    return 0


def _cli_scan(args: argparse.Namespace) -> int:
    """Run local or GCS file scan and print metadata JSON to stdout."""
    gcs_client = storage.Client(project=args.project) if args.project else None
    reports: list[dict] = []
    for file_path in args.files:
        try:
            logger.info("Scanning %s", file_path)
            report = _scan_one_input(
                file_path,
                upload_date=args.upload_date,
                gcs_client=gcs_client,
            )
            meta = report.get("metadata", {})
            logger.info(
                "Scan complete: %s (%s records, %s countries)",
                file_path,
                meta.get("record-count", "?"),
                meta.get("country-count", "?"),
            )
            reports.append(report)
        except FileNotFoundError as exc:
            logger.error("%s", exc)
            return 1
        except ValueError as exc:
            logger.error("%s: %s", file_path, exc)
            return 1
        except Exception as exc:  # noqa: BLE001 - surface GCS/auth errors to CLI user
            logger.error("%s: %s", file_path, exc)
            return 1

    if len(reports) == 1:
        _print_cli_json(reports[0])
    else:
        for report in reports:
            print(json.dumps(report, sort_keys=True))
    logger.info("Scan finished: %d file(s)", len(reports))
    return 0


def main(argv: Optional[list[str]] = None) -> int:
    """CLI entry point: scan, ingest, and archive."""
    parser = argparse.ArgumentParser(
        description="SEO listing GCS ingest — scan, ingest, archive",
        epilog="ingest/archive default to dry-run; pass --apply to mutate GCS.",
    )
    _add_log_level_arg(parser)
    subparsers = parser.add_subparsers(dest="command", required=True)

    scan_parser = subparsers.add_parser(
        "scan",
        help="Scan NDJSON (.json or .json.gz) from disk or GCS and print metadata JSON",
    )
    scan_parser.add_argument(
        "files",
        nargs="+",
        help="Local paths and/or gs://bucket/object URIs to scan",
    )
    scan_parser.add_argument(
        "--upload-date",
        metavar="YYYYMMDD",
        help="Override upload-date metadata (default: file mtime or blob time_created UTC)",
    )
    scan_parser.add_argument(
        "--project",
        metavar="GCP_PROJECT",
        help="GCP project for GCS client (default: ADC project)",
    )
    scan_parser.set_defaults(func=_cli_scan)

    ingest_parser = subparsers.add_parser(
        "ingest",
        help="Ingest uploads/ → stg_to_load/ + archive_raw/ (dry-run unless --apply)",
    )
    _add_ingest_args(ingest_parser)
    ingest_parser.set_defaults(func=_cli_ingest)

    promote_parser = subparsers.add_parser(
        "promote",
        help="Deprecated alias for ingest",
    )
    _add_ingest_args(promote_parser)
    promote_parser.set_defaults(func=_cli_promote)

    archive_parser = subparsers.add_parser(
        "archive",
        help="Move stg_to_load/ → archive_ingested/ (dry-run unless --apply)",
    )
    _add_ingest_args(archive_parser)
    archive_parser.set_defaults(func=_cli_archive)

    args = parser.parse_args(argv)
    _setup_cli_logging(getattr(logging, args.log_level))
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
