"""Reservation Tool table config for incremental Cloud SQL export.

Catalog is trimmed from production (~50 tables) to a representative
set that still shows the engineering rules:

  INCREMENTAL — auto_increment ``id`` + date/time columns.
                Daily: ``WHERE id > max(id)``. Weekly (Sunday): full.
  FULL_LOAD   — composite keys or no usable date watermark.
                Always full export + WRITE_TRUNCATE.

Cloud SQL CSV NULL handling
---------------------------
Cloud SQL MySQL writes NULLs as ``\\N``. BigQuery's CSV parser treats
the backslash as an escape and corrupts field boundaries. Every column
in the export SELECT is wrapped in ``IFNULL(col, '')``; dbt restores
NULLs with ``NULLIF(col, '')``.

Source (read-only):
  dags/horeca_digital/rt_table_config.py
"""

from __future__ import annotations

INCREMENTAL_TABLES = [
    "reservations",  # largest fact — creation/start/end/cancellation dates
    "customers",  # guest profiles — creation_date
    "establishments",  # venue master — creation_date, deletion_date
    "feedback",  # post-visit ratings — creation_date
    "users",  # staff logins — creation_date, deletion_date
]

FULL_LOAD_TABLES = [
    "reservations2reservables",  # composite key, no auto_increment
    "reservables",  # table / seat inventory
    "tenants",  # tiny reference
    "customerconsent",  # composite key (customer_id)
]

# bit(1) / tinyint(1): Cloud SQL CSV can emit binary NUL for false.
# CAST(... AS UNSIGNED) forces ASCII 0/1.
EXPORT_CAST_UNSIGNED_COLUMNS: frozenset[tuple[str, str]] = frozenset(
    {
        ("establishments", "widget_enabled"),
        ("establishments", "test_establishment"),
        ("establishments", "establishment_deleted"),
        ("establishments", "sms_notification_enabled"),
        ("customers", "anonymized"),
        ("feedback", "recommendable"),
        ("reservations", "has_feedback"),
        ("reservations", "feedback_invitation_sent"),
        ("users", "confirmed"),
        ("users", "user_deleted"),
        ("customerconsent", "double_opt_in_completed"),
    }
)

# Credentials / tokens never leave MySQL in this path.
EXCLUDE_COLUMNS = {
    "users": {"password", "password_reset_token"},
    "establishments": {"facebook_access_token"},
}

# Free-text columns that embed newlines / quotes and break BQ CSV parse.
_TEXT_COLUMNS: frozenset[tuple[str, str]] = frozenset(
    {
        ("customers", "note"),
        ("customers", "address"),
        ("establishments", "description"),
        ("establishments", "data_privacy_policy"),
        ("feedback", "text"),
        ("feedback", "response"),
        ("reservables", "description"),
        ("reservations", "internal_comment"),
    }
)

_TABLE_COLUMNS: dict[str, list[str]] = {
    "reservations": [
        "id",
        "capacity",
        "creation_date",
        "email",
        "end_date",
        "feedback_invitation_sent",
        "has_feedback",
        "internal_comment",
        "phone_number",
        "start_date",
        "status",
        "customer_id",
        "establishment_id",
        "source",
        "cancellation_date",
        "creator_id",
    ],
    "customers": [
        "id",
        "anonymized",
        "creation_date",
        "email",
        "first_name",
        "last_name",
        "phone_number",
        "establishment_id",
        "address",
        "note",
    ],
    "establishments": [
        "id",
        "capacity",
        "city",
        "country_code",
        "creation_date",
        "data_privacy_policy",
        "deletion_date",
        "description",
        "email",
        "establishment_deleted",
        "name",
        "phone_number",
        "salesforce_id",
        "test_establishment",
        "time_zone",
        "url",
        "widget_enabled",
        "zip_code",
        "tenant_id",
        "sms_notification_enabled",
    ],
    "feedback": [
        "id",
        "creation_date",
        "email",
        "rating_food_beverage",
        "rating_service",
        "text",
        "establishment_id",
        "reservation_id",
        "status",
        "recommendable",
        "response",
        "response_date",
    ],
    "users": [
        "id",
        "confirmed",
        "country_code",
        "creation_date",
        "deletion_date",
        "email",
        "first_name",
        "last_login_date",
        "last_name",
        "login",
        # password / password_reset_token excluded
        "phone_number",
        "salesforce_id",
        "user_deleted",
        "tenant_id",
    ],
    "reservations2reservables": [
        "reservation_id",
        "reservable_id",
    ],
    "reservables": [
        "id",
        "capacity",
        "description",
        "name",
        "sort_position",
        "reservable_group_id",
    ],
    "tenants": [
        "id",
        "code",
        "name",
    ],
    "customerconsent": [
        "customer_id",
        "consent_version",
        "double_opt_in_completed",
    ],
}


def _mysql_ident(col: str) -> str:
    """Quote a MySQL identifier (handles reserved words like ``text``)."""
    return f"`{col}`"


def _sanitise_text_expr(quoted_col: str) -> str:
    """Replace embedded newlines and double-quotes before CSV export.

    Cloud SQL emits MySQL-style ``\\\"`` escapes that BigQuery's CSV
    parser does not understand even with ``allow_quoted_newlines``.
    """
    return (
        "REPLACE(REPLACE(REPLACE("
        f"{quoted_col}"
        ",CHAR(10),' ')"  # \n → space
        ",CHAR(13),' ')"  # \r → space
        ",'\"','''')"  # " → '
    )


def build_select_query(table: str) -> str:
    """Build a MySQL SELECT safe for Cloud SQL Admin CSV export.

    * Every column wrapped in ``IFNULL(..., '')`` (avoids ``\\N``).
    * Bit flags cast to UNSIGNED.
    * Free-text columns get newline / quote sanitisation.
    * ``EXCLUDE_COLUMNS`` are never present in ``_TABLE_COLUMNS``.
    """
    columns = _TABLE_COLUMNS.get(table)
    if columns is None:
        raise ValueError(
            f"Table {table!r} has no column list in _TABLE_COLUMNS — "
            "add it before using build_select_query."
        )

    excluded = EXCLUDE_COLUMNS.get(table, set())
    parts: list[str] = []
    for col in columns:
        if col in excluded:
            continue
        q = _mysql_ident(col)
        if (table, col) in EXPORT_CAST_UNSIGNED_COLUMNS:
            parts.append(f"IFNULL(CAST({q} AS UNSIGNED), '') AS {q}")
        elif (table, col) in _TEXT_COLUMNS:
            parts.append(f"IFNULL({_sanitise_text_expr(q)}, '') AS {q}")
        else:
            # REPLACE is a no-op for numeric / timestamp values and
            # still protects against quote-escape surprises.
            parts.append(f"IFNULL({_sanitise_text_expr(q)}, '') AS {q}")
    return f"SELECT {', '.join(parts)} FROM `{table}`"
