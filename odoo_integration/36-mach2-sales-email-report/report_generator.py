"""Generate Mach2 Excel reports and stage them for email delivery."""

from __future__ import annotations

import importlib.util
import logging
import re
import sys
from pathlib import Path

from config import (
    MACH2_ENTRYPOINT,
    MACH2_SOURCE_DIR,
    MACH2_STAGING_DIR,
    load_mach2_environment,
)

log = logging.getLogger(__name__)


def _resolve_source_dir() -> Path:
    composer = Path(MACH2_SOURCE_DIR)
    if (composer / MACH2_ENTRYPOINT).is_file():
        return composer
    # Portfolio layout: report.py lives beside this module.
    local = Path(__file__).resolve().parent
    if (local / MACH2_ENTRYPOINT).is_file():
        return local
    raise FileNotFoundError(
        f"Mach2 entrypoint not found in {composer} or {local}"
    )


def _load_report_module():
    load_mach2_environment()
    source_dir = _resolve_source_dir()
    entrypoint = source_dir / MACH2_ENTRYPOINT

    source_dir_str = str(source_dir)
    if source_dir_str not in sys.path:
        sys.path.insert(0, source_dir_str)

    spec = importlib.util.spec_from_file_location("mach2_report_main", entrypoint)
    if spec is None or spec.loader is None:
        raise ImportError(f"Unable to load Mach2 entrypoint: {entrypoint}")

    module = importlib.util.module_from_spec(spec)
    sys.modules["mach2_report_main"] = module
    spec.loader.exec_module(module)
    return module


def _staging_dir_for_run(run_id: str) -> Path:
    safe_run_id = re.sub(r"[^\w\-.]+", "_", run_id)
    composer_root = Path(MACH2_STAGING_DIR)
    try:
        composer_root.mkdir(parents=True, exist_ok=True)
        root = composer_root
    except OSError:
        root = Path("/tmp/mach2_reports")
        root.mkdir(parents=True, exist_ok=True)
    return root / safe_run_id


def generate_mach2_reports(**context) -> list[dict]:
    """Airflow callable: run BQ queries, write Excel files, return email payloads."""
    module = _load_report_module()
    run_id = str(context.get("run_id", "manual"))
    output_dir = _staging_dir_for_run(run_id)
    output_dir.mkdir(parents=True, exist_ok=True)

    log.info("Staging Mach2 reports in %s", output_dir)
    reports = module.generate_reports(output_dir)
    log.info("Generated %d report payload(s).", len(reports))
    return reports
