"""Date-range resolution for overnight POS land.

Default: yesterday only (daily mode).
Backfill: uncomment BACKFILL_START_DATE / BACKFILL_END_DATE in the DAG
module (or set Airflow Variables) and the same load/move callables walk
each day in the closed range inside one DAG run.

Production used module-level constants + NameError to toggle modes.
Here we prefer Variables so a backfill does not require editing code,
with an optional module override for local experiments.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import List, Optional

from airflow.models import Variable

# Optional code-level override (keep None for Variable / daily default).
BACKFILL_START_DATE: Optional[str] = None  # "YYYY-MM-DD"
BACKFILL_END_DATE: Optional[str] = None


def get_date_range(
    backfill_start: Optional[str] = None,
    backfill_end: Optional[str] = None,
) -> List[str]:
    """Return YYYYMMDD strings to process (one day or a closed range)."""
    start_raw = (
        backfill_start
        or BACKFILL_START_DATE
        or Variable.get("pos_overnight_backfill_start", default_var="")
        or None
    )
    end_raw = (
        backfill_end
        or BACKFILL_END_DATE
        or Variable.get("pos_overnight_backfill_end", default_var="")
        or None
    )

    if start_raw and end_raw:
        start = datetime.strptime(start_raw, "%Y-%m-%d").date()
        end = datetime.strptime(end_raw, "%Y-%m-%d").date()
        if end < start:
            raise ValueError(f"backfill end {end} before start {start}")
        out: List[str] = []
        cursor = start
        while cursor <= end:
            out.append(cursor.strftime("%Y%m%d"))
            cursor += timedelta(days=1)
        return out

    yesterday: date = (datetime.now() - timedelta(days=1)).date()
    return [yesterday.strftime("%Y%m%d")]


def is_backfill_mode() -> bool:
    dates = get_date_range()
    return len(dates) > 1
