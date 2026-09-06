"""
templates/pos_activation.py
───────────────────────────
Email template for the "POS Activation" report.
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
    sheet_name: str = "Provisioning",
) -> str:
    """Return the fully rendered HTML email for the POS Activation report."""

    body_inner = f"""
      <p>
        Please find attached the <strong>POS Activation</strong> report
        for <strong>{run_date}</strong>.
      </p>
    """

    return render(
        title="Provisioning Report",
        badge="MACH2 · NEW CUSTOMER PROVISIONING",
        body_inner=body_inner,
        run_date=run_date,
        run_ts=run_ts,
        row_count=len(df),
        col_count=len(df.columns),
        column_names=", ".join(df.columns.tolist()),
        sheet_name=sheet_name,
        filename=filename,
    )
