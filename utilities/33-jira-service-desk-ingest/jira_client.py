"""Jira Cloud issue extract with ADF flattening and token pagination.

Pulls issues (fields + changelog + comments) for one or more projects,
writes JSONL to a Composer-local path. Credentials come from an Airflow
Variable at task runtime — never at DAG parse time.

Production quirks kept on purpose:
- Intermediate JSONL every 10k rows so a killed task does not lose hours
- Conservative sleep between pages; hard wait on 429 / 5xx
- Approximate-count preflight so we can skip empty windows cleanly

Source (read-only): dags/horeca_digital/jira_hdsd.py
"""

from __future__ import annotations

import glob
import json
import logging
import os
import time
from datetime import date, timedelta

import requests
from airflow.models import Variable
from requests.auth import HTTPBasicAuth

logger = logging.getLogger(__name__)

# Overridden via Variable in real deploys.
DEFAULT_JIRA_BASE_URL = "https://example.atlassian.net"
HEADERS = {"Content-Type": "application/json", "Accept": "application/json"}


def _jira_base_url() -> str:
    return Variable.get("jira_base_url", default_var=DEFAULT_JIRA_BASE_URL)


def _get_jira_creds() -> tuple[str, str]:
    """Username + API token from Airflow Variable (task runtime only)."""
    creds = Variable.get("jira_service_desk_creds", deserialize_json=True)
    return creds["username"], creds["api_token"]


def extract_text(field_body) -> str:
    """Flatten Atlassian Document Format (ADF) into a single string.

    Walks `content[].text` and mention `attrs.text`. Nested content is
    joined with spaces. Non-dict/list leaves become empty.
    """
    parts: list[str] = []

    if isinstance(field_body, dict):
        if "text" in field_body:
            parts.append(field_body["text"])
        if "attrs" in field_body and "text" in field_body["attrs"]:
            parts.append(field_body["attrs"]["text"])
        if "content" in field_body:
            for node in field_body["content"]:
                parts.append(extract_text(node))
    elif isinstance(field_body, list):
        for item in field_body:
            parts.append(extract_text(item))

    return " ".join(p for p in parts if p)


def format_comments(comments: list[dict]) -> str:
    """Serialize comment dicts into a stable multi-line block for JSONL."""
    blocks = []
    for i, comment in enumerate(comments, start=1):
        blocks.append(
            f"Comment {i}\n"
            f"'author': {comment['author']},\n"
            f"'created': {comment['created']},\n"
            f"'body': {comment['body']}\n"
            "=================="
        )
    return "\n".join(blocks)


def _normalize_issue(issue: dict) -> dict:
    fields = issue.get("fields") or {}
    description = fields.get("description") or ""
    cleaned_description = (
        extract_text(description).replace("\r", "").replace("\n", " ")
        if description
        else ""
    )

    raw_comments = (fields.get("comment") or {}).get("comments") or []
    extracted = [
        {
            "author": c["author"]["displayName"],
            "created": c["created"],
            "body": extract_text(c["body"]).replace("\r", "").replace("\n", " "),
        }
        for c in raw_comments
    ]

    changelog = (issue.get("changelog") or {}).get("histories") or []

    return {
        "key": issue["key"],
        "fields": fields,
        "description": cleaned_description,
        "changelog": changelog,
        "comments": format_comments(extracted),
    }


def get_jira_issue_count(project_key: str, jql: str) -> int:
    """Approximate issue count for a JQL (preflight / progress ETA)."""
    username, api_token = _get_jira_creds()
    url = f"{_jira_base_url()}/rest/api/3/search/approximate-count"
    response = requests.post(
        url,
        data=json.dumps({"jql": jql}),
        headers=HEADERS,
        auth=HTTPBasicAuth(username, api_token),
        timeout=60,
    )

    if response.status_code == 200:
        total = response.json().get("count", 0)
        logger.info("Project %s count for JQL: %s", project_key, total)
        return int(total)
    if response.status_code == 401:
        raise RuntimeError(
            f"Jira auth failed (401) for project {project_key}; "
            "rotate the API token in Airflow Variable jira_service_desk_creds."
        )
    if response.status_code == 403:
        raise RuntimeError(
            f"Jira forbidden (403) for project {project_key}; "
            "check project browse permissions for the service account."
        )
    raise RuntimeError(
        f"Jira count failed ({response.status_code}) for {project_key}: "
        f"{response.text[:200]}"
    )


def get_jira_project_date_range(project_key: str):
    """Return (earliest_created, latest_updated) as YYYY-MM-DD, or None.

    Used at DAG parse time in FULL_LOAD_MODE to size monthly TaskGroups
    from real project history instead of hardcoded year lists.
    """
    username, api_token = _get_jira_creds()
    url = f"{_jira_base_url()}/rest/api/3/search/jql"
    auth = HTTPBasicAuth(username, api_token)

    def _one(jql: str, field: str) -> str | None:
        resp = requests.get(
            url,
            headers={"Accept": "application/json"},
            params={"jql": jql, "maxResults": 1, "fields": field},
            auth=auth,
            timeout=60,
        )
        if resp.status_code == 401:
            raise RuntimeError(
                f"Jira auth failed (401) probing date range for {project_key}"
            )
        if resp.status_code != 200:
            raise RuntimeError(
                f"Jira date-range probe failed ({resp.status_code}) for {project_key}"
            )
        issues = resp.json().get("issues") or []
        if not issues:
            return None
        return issues[0]["fields"][field].split("T")[0]

    earliest = _one(f'project="{project_key}" ORDER BY created ASC', "created")
    latest = _one(f'project="{project_key}" ORDER BY updated DESC', "updated")

    if earliest and latest:
        logger.info("Date range for %s: %s → %s", project_key, earliest, latest)
        return earliest, latest
    logger.warning("Could not determine date range for %s", project_key)
    return None


def get_jira_issues_by_date_range(
    project_key: str,
    start_date,
    end_date,
    destination_path: str,
    **_context,
) -> int:
    """Extract issues for a project/window into JSONL.

    start_date=None → full project extract (no updated filter).
    Uses nextPageToken pagination on /rest/api/3/search/jql.
    """
    username, api_token = _get_jira_creds()
    auth = HTTPBasicAuth(username, api_token)

    if start_date is None:
        jql = f'project="{project_key}"'
        logger.info("Full-load extract for %s", project_key)
    else:
        jql = (
            f'project="{project_key}" AND updated >= "{start_date}" '
            f'AND updated <= "{end_date}"'
        )
        logger.info("Window extract for %s: %s → %s", project_key, start_date, end_date)

    os.makedirs(os.path.dirname(destination_path) or ".", exist_ok=True)

    issue_count = get_jira_issue_count(project_key, jql)
    if issue_count == 0:
        open(destination_path, "w", encoding="utf-8").close()
        logger.info("Empty window — wrote %s", destination_path)
        return 0

    url = f"{_jira_base_url()}/rest/api/3/search/jql"
    max_results = 100  # keep pages small; Jira Cloud timeouts are real
    delay_seconds = 2
    issues_data: list[dict] = []
    next_page_token = None
    started = time.time()

    while True:
        query = {
            "jql": jql,
            "expand": "changelog",
            "fields": "*all",
            "maxResults": max_results,
        }
        if next_page_token:
            query["nextPageToken"] = next_page_token

        response = requests.get(
            url, headers=HEADERS, params=query, auth=auth, timeout=120
        )

        if response.status_code == 429:
            logger.warning("Rate limited — sleeping 60s")
            time.sleep(60)
            continue
        if response.status_code >= 500:
            logger.warning("Server %s — sleeping 30s", response.status_code)
            time.sleep(30)
            continue
        if response.status_code != 200:
            logger.error(
                "Fetch failed %s: %s", response.status_code, response.text[:300]
            )
            return len(issues_data)

        payload = response.json()
        page = payload.get("issues") or []
        next_page_token = payload.get("nextPageToken")

        if not page:
            break

        for issue in page:
            issues_data.append(_normalize_issue(issue))

        elapsed = time.time() - started
        rate = len(issues_data) / elapsed if elapsed else 0
        logger.info(
            "Progress %s/%s (%.1f%%) @ %.2f issues/s",
            len(issues_data),
            issue_count,
            (len(issues_data) / issue_count) * 100 if issue_count else 0,
            rate,
        )

        # Checkpoint so a Composer preempt does not burn a full re-pull.
        if len(issues_data) % 10000 == 0 and len(issues_data) > 0:
            temp_path = destination_path.replace(
                ".jsonl", f"_temp_{len(issues_data)}.jsonl"
            )
            with open(temp_path, "w", encoding="utf-8") as tmp:
                for row in issues_data:
                    tmp.write(json.dumps(row, ensure_ascii=False) + "\n")
            logger.info("Checkpoint written: %s", temp_path)

        if not next_page_token:
            break
        time.sleep(delay_seconds)

    with open(destination_path, "w", encoding="utf-8") as out:
        for row in issues_data:
            out.write(json.dumps(row, ensure_ascii=False) + "\n")

    for temp_file in glob.glob(
        destination_path.replace(".jsonl", "_temp_*.jsonl")
    ):
        try:
            os.remove(temp_file)
        except OSError:
            pass

    logger.info(
        "Wrote %s issues to %s in %.1f min",
        len(issues_data),
        destination_path,
        (time.time() - started) / 60,
    )
    return len(issues_data)


def get_jira_issues_batch(
    project_key: str,
    start_date,
    end_date,
    destination_path: str,
    start_at: int,
    max_results: int,
) -> int:
    """Legacy startAt/maxResults batch helper (classic /search).

    Prefer get_jira_issues_by_date_range for new work — Jira is moving
    search to nextPageToken. Kept because some full-load experiments
    still called this path.
    """
    username, api_token = _get_jira_creds()
    if start_date is None:
        jql = f'project="{project_key}"'
    else:
        jql = (
            f'project="{project_key}" AND updated >= "{start_date}" '
            f'AND updated <= "{end_date}"'
        )

    os.makedirs(os.path.dirname(destination_path) or ".", exist_ok=True)
    url = f"{_jira_base_url()}/rest/api/3/search"
    query = {
        "jql": jql,
        "expand": "changelog",
        "fields": "*all",
        "startAt": start_at,
        "maxResults": max_results,
    }

    response = requests.get(
        url,
        headers=HEADERS,
        auth=HTTPBasicAuth(username, api_token),
        params=query,
        timeout=120,
    )

    if response.status_code == 429:
        time.sleep(60)
        return get_jira_issues_batch(
            project_key, start_date, end_date, destination_path, start_at, max_results
        )
    if response.status_code >= 500:
        time.sleep(30)
        return get_jira_issues_batch(
            project_key, start_date, end_date, destination_path, start_at, max_results
        )
    if response.status_code != 200:
        logger.error("Batch fetch failed: %s", response.status_code)
        return 0

    issues_data = [_normalize_issue(i) for i in response.json().get("issues") or []]
    time.sleep(2)

    with open(destination_path, "w", encoding="utf-8") as out:
        for row in issues_data:
            out.write(json.dumps(row, ensure_ascii=False) + "\n")
    return len(issues_data)


# --- deprecated thin wrapper kept for call-site compatibility ---

def get_last_1_days_issues(project_key, execution_date, previous_execution_date=None):
    """Deprecated. Prefer get_jira_issues_by_date_range."""
    small = project_key.lower()
    clean = str(execution_date).replace(":", "-").replace(" ", "_")
    path = f"/home/airflow/gcs/data/jira_{small}/{project_key}_{clean}.jsonl"
    return get_jira_issues_by_date_range(
        project_key,
        previous_execution_date,
        execution_date,
        path,
    )


if __name__ == "__main__":
    # Local smoke — expects Variable / env wiring; not for CI.
    logging.basicConfig(level=logging.INFO)
    demo_project = "SUP"
    demo_end = date.today()
    demo_start = demo_end - timedelta(days=1)
    get_jira_issues_by_date_range(
        demo_project,
        demo_start.strftime("%Y-%m-%d"),
        demo_end.strftime("%Y-%m-%d"),
        f"./jira_extracts/{demo_project}_demo.jsonl",
    )
