"""
templates/pos_cancellation.py
───────────────────────────────
Email template for the "POS Cancellation" report.
"""

from __future__ import annotations

import pandas as pd
from templates.base import render


def build(
    *,
    df: pd.DataFrame,
    run_date: str,
    run_ts: str,
    filename: str,
    sheet_name: str = "POS Cancellation",
) -> str:
    """Return the fully rendered HTML email for the POS Cancellation report."""

    body_inner = f"""
      <p>
        Please find attached the <strong>POS Cancellation</strong> report
        for <strong>{run_date}</strong>.
      </p>
    """

    return render(
        title="POS Cancellation Report",
        badge="POS · CANCELLATION",
        body_inner=body_inner,
        run_date=run_date,
        run_ts=run_ts,
        row_count=len(df),
        col_count=len(df.columns),
        column_names=", ".join(df.columns.tolist()),
        sheet_name=sheet_name,
        filename=filename,
    )
