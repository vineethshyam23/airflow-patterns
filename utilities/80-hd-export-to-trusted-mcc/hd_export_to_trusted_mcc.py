"""
Helpers for etl_hd_export_to_trusted_mcc.

- copy_country: materialize every view/table in export_{cnt} into
  trusted_mcc.{cnt}_{name} via the BigQuery client; return failures
  as a list of {"view", "error"} dicts (plain XCom).
- build_slack_message: DAG Failure / completed with failed views /
  Success text for the Slack API post operator.
"""

from __future__ import annotations

import logging

from google.api_core.exceptions import GoogleAPIError
from google.cloud import bigquery

logger = logging.getLogger(__name__)

# Sanitized project / dataset ids — set via Variables in real Composer.
SOURCE_PROJECT_ID = "export_project"
DEST_PROJECT_ID = "dwh_project"
DEST_DATASET_ID = "trusted_mcc"

# Known views we cannot access (permission / private); skip in Slack.
SKIP_FAILED_VIEWS = frozenset({"bel_rt_private_label"})

MAX_TASKS_IN_MESSAGE = 5
MAX_VIEWS_IN_MESSAGE = 30
MAX_ERROR_CHARS = 200


def copy_country(cnt: str, client=None) -> list[dict]:
    """
    Copy all objects of export_{cnt} (source views) into destination
    tables (WRITE_TRUNCATE).

    A failing view is recorded and the loop continues. If the source
    dataset cannot be listed (missing / no permission) the country is
    skipped — same contract as the previous bash script this replaced.
    """
    client = client or bigquery.Client()
    source_dataset = f"{SOURCE_PROJECT_ID}.export_{cnt}"
    try:
        tables = list(client.list_tables(source_dataset, max_results=1000))
    except GoogleAPIError as e:
        logger.warning(
            "Cannot list %s, skipping: %s",
            source_dataset,
            getattr(e, "message", None) or e,
        )
        tables = []
    if not tables:
        logger.info("No tables found in export_%s", cnt)
        return []
    logger.info(
        "Starting copy for %s: %s objects in %s -> %s.%s",
        cnt,
        len(tables),
        source_dataset,
        DEST_PROJECT_ID,
        DEST_DATASET_ID,
    )

    failed = []
    for table in tables:
        name = table.table_id
        dest_name = f"{cnt}_{name}"
        job_config = bigquery.QueryJobConfig(
            destination=f"{DEST_PROJECT_ID}.{DEST_DATASET_ID}.{dest_name}",
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )
        try:
            client.query(
                f"SELECT * FROM `{source_dataset}.{name}`",
                job_config=job_config,
            ).result()
            logger.info("OK: %s.%s", cnt, name)
        except GoogleAPIError as e:
            error = getattr(e, "message", None) or str(e)
            logger.warning("SKIPPED (error): %s.%s: %s", cnt, name, error)
            failed.append({"view": dest_name, "error": error})
    logger.info("Done. %s failed of %s.", len(failed), len(tables))
    return failed


def _slack_escape(text: str) -> str:
    """Escape Slack control chars; drop backticks/newlines that break inline code."""
    text = " ".join(text.split()).replace("`", "'")
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _format_failed_views(failed_views: list[dict]) -> str:
    shown = failed_views[:MAX_VIEWS_IN_MESSAGE]
    lines = [f"*Failed views ({len(failed_views)}):*"]
    for v in shown:
        error = _slack_escape(v.get("error") or "-")
        if len(error) > MAX_ERROR_CHARS:
            error = error[:MAX_ERROR_CHARS] + "..."
        lines.append(f"• `{_slack_escape(v['view'])}` — {error}")
    if len(failed_views) > len(shown):
        lines.append(
            f"_(+{len(failed_views) - len(shown)} more, see task logs)_"
        )
    return "\n".join(lines)


def collect_failed_views(per_task_results) -> list[dict]:
    """Flatten per-country XCom lists; drop SKIP_FAILED_VIEWS."""
    return [
        v
        for result in per_task_results or []
        for v in result or []
        if v.get("view") not in SKIP_FAILED_VIEWS
    ]


def build_slack_message(
    dag_id: str, failed_task_ids: list[str], failed_views: list[dict]
) -> str:
    if failed_task_ids:
        shown = failed_task_ids[:MAX_TASKS_IN_MESSAGE]
        more = len(failed_task_ids) - len(shown)
        suffix = f" (+{more} more)" if more else ""
        message = (
            f"*DAG Failure*\n\nDAG: `{dag_id}`\n"
            f"Failed task(s): `{', '.join(shown)}{suffix}`"
        )
        if failed_views:
            message += "\n\n" + _format_failed_views(failed_views)
        return message
    if failed_views:
        return (
            f"*DAG completed with failed views*\n\nDAG: `{dag_id}`\n\n"
            + _format_failed_views(failed_views)
        )
    return f"*DAG Success*\n\n`{dag_id}` completed successfully."
