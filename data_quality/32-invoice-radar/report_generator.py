"""Generate the Invoice Radar Excel report and stage it for email delivery."""

from __future__ import annotations

import importlib.util
import logging
import re
import sys
from pathlib import Path

from config import (
    INVOICE_RADAR_ENTRYPOINT,
    INVOICE_RADAR_SOURCE_DIR,
    INVOICE_RADAR_STAGING_DIR,
    load_invoice_radar_environment,
)

log = logging.getLogger(__name__)


def _resolve_source_dir() -> Path:
    composer = Path(INVOICE_RADAR_SOURCE_DIR)
    if (composer / INVOICE_RADAR_ENTRYPOINT).is_file():
        return composer
    # Portfolio layout: invoice_radar.py lives beside this module.
    local = Path(__file__).resolve().parent
    if (local / INVOICE_RADAR_ENTRYPOINT).is_file():
        return local
    raise FileNotFoundError(
        f"Invoice Radar entrypoint not found in {composer} or {local}"
    )


def _load_report_module():
    load_invoice_radar_environment()
    source_dir = _resolve_source_dir()
    entrypoint = source_dir / INVOICE_RADAR_ENTRYPOINT
    if not entrypoint.exists():
        raise FileNotFoundError(f"Invoice Radar entrypoint not found: {entrypoint}")

    source_dir_str = str(source_dir)
    if source_dir_str not in sys.path:
        sys.path.insert(0, source_dir_str)

    spec = importlib.util.spec_from_file_location("invoice_radar_main", entrypoint)
    if spec is None or spec.loader is None:
        raise ImportError(f"Unable to load Invoice Radar entrypoint: {entrypoint}")

    module = importlib.util.module_from_spec(spec)
    sys.modules["invoice_radar_main"] = module
    spec.loader.exec_module(module)
    return module


def _staging_dir_for_run(run_id: str) -> Path:
    safe_run_id = re.sub(r"[^\w\-.]+", "_", run_id)
    composer_root = Path(INVOICE_RADAR_STAGING_DIR)
    try:
        composer_root.mkdir(parents=True, exist_ok=True)
        root = composer_root
    except OSError:
        root = Path("/tmp/invoice_radar")
        root.mkdir(parents=True, exist_ok=True)
    return root / safe_run_id


def generate_invoice_radar_reports(**context) -> list[dict]:
    """Airflow callable: run BQ queries, write Excel, return email payloads."""
    module = _load_report_module()
    run_id = str(context.get("run_id", "manual"))
    output_dir = _staging_dir_for_run(run_id)
    output_dir.mkdir(parents=True, exist_ok=True)

    log.info("Staging Invoice Radar report in %s", output_dir)
    reports = module.generate_reports(output_dir)
    log.info("Generated %d report payload(s).", len(reports))
    return reports
