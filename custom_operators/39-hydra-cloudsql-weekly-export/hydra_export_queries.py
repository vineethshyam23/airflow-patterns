"""Hydra raw Cloud SQL → CSV export query helpers.

Production catalogs ~80 website tables. This reference keeps the
engineering core (CSV-safe SELECT builders, MySQL→BQ type map, schema
JSON / dbt stub generators) plus a small representative table set so
the pattern stays reviewable. Expand ``HYDRA_RAW_TABLES`` from
information_schema when you wire a real instance.

Cloud SQL CSV export mishandles NULLs and embedded newlines
(Google known issues). We sanitize in SQL instead of relying on
``escapeCharacter``.

Source (read-only):
  dags/horeca_digital/hydra_raw_export_queries.py
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import NamedTuple

# Line-break replacement in string columns during Cloud SQL CSV export.
HYDRA_CSV_NEWLINE_PLACEHOLDER = "<br />"

# Credential columns stripped from generated dbt staging views.
HYDRA_STG_EXCLUDED_COLUMNS: dict[str, frozenset[str]] = {
    "users": frozenset({"password", "password_reset_token"}),
}


class ColumnSpec(NamedTuple):
    """One exported column: MySQL identifier + COLUMN_TYPE."""

    name: str
    mysql_type: str


class HydraRawTable(NamedTuple):
    """One exportable table: name, columns, optional MySQL PK."""

    name: str
    columns: tuple[ColumnSpec, ...]
    primary_key: tuple[str, ...] = ()


def hydra_column_names(spec: HydraRawTable) -> tuple[str, ...]:
    return tuple(c.name for c in spec.columns)


def _mysql_type_is_bit(column_type: str) -> bool:
    t = column_type.strip().lower()
    return t == "bit" or t.startswith("bit(")


def _mysql_type_is_string_column(column_type: str) -> bool:
    if _mysql_type_is_bit(column_type):
        return False
    return mysql_column_type_to_bq_load_schema(column_type) == "STRING"


def _csv_safe_string_sql_expr(column_name: str) -> str:
    """Sanitize one string column for Cloud SQL CSV → BigQuery load.

    - IFNULL → empty field (avoid ``\\N`` / broken ``"N`` quotes)
    - Double embedded double-quotes (RFC4180)
    - CR/LF → newline placeholder so each logical row is one CSV line
    """
    ph = HYDRA_CSV_NEWLINE_PLACEHOLDER.replace("'", "''")
    expr = f"IFNULL(`{column_name}`, '')"
    expr = f"REPLACE({expr}, CHAR(34), CONCAT(CHAR(34), CHAR(34)))"
    for needle in ("\\r\\n", "\\n\\r", "\\r", "\\n"):
        expr = f"REPLACE({expr}, '{needle}', '{ph}')"
    expr = f"REPLACE({expr}, CHAR(13), '{ph}')"
    expr = f"REPLACE({expr}, CHAR(10), '{ph}')"
    return expr


def _export_column_sql(col: ColumnSpec) -> str:
    if _mysql_type_is_bit(col.mysql_type):
        expr = f"CAST(`{col.name}` AS UNSIGNED)"
        return f"IFNULL({expr}, '') AS `{col.name}`"
    if _mysql_type_is_string_column(col.mysql_type):
        return f"{_csv_safe_string_sql_expr(col.name)} AS `{col.name}`"
    return f"IFNULL(`{col.name}`, '') AS `{col.name}`"


def _select_sql(spec: HydraRawTable) -> str:
    col_sql = ", ".join(_export_column_sql(c) for c in spec.columns)
    return f"SELECT {col_sql} FROM `{spec.name}`"


def mysql_column_type_to_bq_load_schema(column_type: str) -> str:
    """BigQuery load type after export SQL transforms (bit → INT64)."""
    if _mysql_type_is_bit(column_type):
        return "INT64"
    t = column_type.strip().lower()
    base = t.split("(", 1)[0].strip()

    if base in ("decimal", "numeric", "dec"):
        return "NUMERIC"
    if base in ("double", "float", "real"):
        return "FLOAT64"
    if base in ("tinyint", "smallint", "mediumint", "int", "integer", "bigint"):
        return "INT64"
    if base == "date":
        return "DATE"
    if base in ("datetime", "timestamp"):
        return "TIMESTAMP"
    if base == "year":
        return "INT64"
    # time / text / json / blob / geometry → STRING for CSV safety
    return "STRING"


# ---------------------------------------------------------------------------
# Representative catalog (production had ~80 tables). Keep small + illustrative.
# ---------------------------------------------------------------------------

HYDRA_RAW_TABLES: tuple[HydraRawTable, ...] = (
    HydraRawTable(
        "countries",
        (
            ColumnSpec("id", "bigint"),
            ColumnSpec("code", "varchar(16)"),
            ColumnSpec("iso_code", "varchar(16)"),
            ColumnSpec("default_language", "varchar(5)"),
            ColumnSpec("enabled", "bit(1)"),
            ColumnSpec("sort_position", "int"),
            ColumnSpec("support_email", "varchar(160)"),
        ),
        ("id",),
    ),
    HydraRawTable(
        "users",
        (
            ColumnSpec("id", "bigint"),
            ColumnSpec("email", "varchar(255)"),
            ColumnSpec("password", "varchar(255)"),
            ColumnSpec("password_reset_token", "varchar(255)"),
            ColumnSpec("enabled", "bit(1)"),
            ColumnSpec("creation_date", "datetime"),
            ColumnSpec("last_modification_date", "datetime"),
        ),
        ("id",),
    ),
    HydraRawTable(
        "establishments",
        (
            ColumnSpec("id", "bigint"),
            ColumnSpec("name", "varchar(255)"),
            ColumnSpec("country_id", "bigint"),
            ColumnSpec("enabled", "bit(1)"),
            ColumnSpec("creation_date", "datetime"),
            ColumnSpec("last_modification_date", "datetime"),
            ColumnSpec("latitude", "double"),
            ColumnSpec("longitude", "double"),
        ),
        ("id",),
    ),
    HydraRawTable(
        "users2countries",
        (
            ColumnSpec("user_id", "bigint"),
            ColumnSpec("country_id", "bigint"),
        ),
        ("user_id", "country_id"),
    ),
    # Junction / sequence-style table with no MySQL PK → snapshot uses all cols.
    HydraRawTable(
        "schema_migrations",
        (
            ColumnSpec("version", "varchar(255)"),
        ),
        (),
    ),
)

_RAW_TABLE_SELECT_SQL: dict[str, str] = {
    spec.name: _select_sql(spec) for spec in HYDRA_RAW_TABLES
}

RAW_TABLE_EXPORT_QUERIES: dict[str, str] = dict(_RAW_TABLE_SELECT_SQL)

HYDRA_RAW_PRIMARY_KEYS: dict[str, tuple[str, ...]] = {
    spec.name: spec.primary_key for spec in HYDRA_RAW_TABLES
}

HYDRA_RAW_TABLES_WITHOUT_PRIMARY_KEY: tuple[str, ...] = tuple(
    spec.name for spec in HYDRA_RAW_TABLES if not spec.primary_key
)


def _snapshot_unique_key_columns(spec: HydraRawTable) -> tuple[str, ...]:
    if spec.primary_key:
        return spec.primary_key
    return hydra_column_names(spec)


HYDRA_RAW_SNAPSHOT_UNIQUE_KEYS: dict[str, tuple[str, ...]] = {
    spec.name: _snapshot_unique_key_columns(spec) for spec in HYDRA_RAW_TABLES
}


def snapshot_unique_key(table: str) -> str | tuple[str, ...]:
    uk = HYDRA_RAW_SNAPSHOT_UNIQUE_KEYS[table]
    if len(uk) == 1:
        return uk[0]
    return uk


ACTIVE_HYDRA_RAW_TABLE_KEYS: tuple[str, ...] = tuple(RAW_TABLE_EXPORT_QUERIES.keys())

# Not exported in v1 — need restricted datasets / masking before enabling.
HYDRA_RAW_SENSITIVE_TABLE_KEYS: frozenset[str] = frozenset(
    {
        "establishments_restricted",
        "establishments_confidential",
        "imprintvalues",
    }
)

HYDRA_RAW_OAUTH_TABLE_KEYS: frozenset[str] = frozenset(
    {
        "oauth_access_token",
        "oauth_client_details",
        "oauth_refresh_token",
    }
)

assert not (
    set(ACTIVE_HYDRA_RAW_TABLE_KEYS) & HYDRA_RAW_SENSITIVE_TABLE_KEYS
), "ACTIVE_HYDRA_RAW_TABLE_KEYS must not include sensitive streams"
assert not (
    set(ACTIVE_HYDRA_RAW_TABLE_KEYS) & HYDRA_RAW_OAUTH_TABLE_KEYS
), "ACTIVE_HYDRA_RAW_TABLE_KEYS must not include OAuth tables"
assert len({s.name for s in HYDRA_RAW_TABLES}) == len(HYDRA_RAW_TABLES)
for _spec in HYDRA_RAW_TABLES:
    if not _spec.primary_key:
        continue
    _exported = set(hydra_column_names(_spec))
    _missing = [c for c in _spec.primary_key if c not in _exported]
    assert not _missing, f"{_spec.name}: PK cols missing from export: {_missing}"


def write_hyd_v2_schema_json_files(destination: Path | str) -> None:
    """Emit ``hyd_v2_<table>.json`` BigQuery CSV schema files."""
    dest = Path(destination)
    dest.mkdir(parents=True, exist_ok=True)
    for spec in HYDRA_RAW_TABLES:
        fields = [
            {
                "name": col.name,
                "type": mysql_column_type_to_bq_load_schema(col.mysql_type),
                "mode": "NULLABLE",
            }
            for col in spec.columns
        ]
        path = dest / f"hyd_v2_{spec.name}.json"
        path.write_text(json.dumps(fields, indent=2) + "\n", encoding="utf-8")
        print("wrote", path)


def _dbt_snapshot_unique_key_config(table: str) -> str:
    pk = snapshot_unique_key(table)
    if isinstance(pk, str):
        return f'unique_key="{pk}"'
    inner = ", ".join(f'"{c}"' for c in pk)
    return f"unique_key=[{inner}]"


def _stg_hyd_v2_model_sql(spec: HydraRawTable) -> str:
    excluded = HYDRA_STG_EXCLUDED_COLUMNS.get(spec.name, frozenset())
    select_parts: list[str] = []
    for col in spec.columns:
        if col.name in excluded:
            continue
        quoted = f"`{col.name}`"
        if _mysql_type_is_string_column(col.mysql_type):
            select_parts.append(
                f"    ifnull(cast(`{col.name}` as string), '') as {quoted}"
            )
        else:
            select_parts.append(f"    {quoted}")
    select_body = ",\n".join(select_parts)
    model_name = f"stg_hyd_v2_{spec.name}"
    source_table = f"hyd_v2_{spec.name}"
    return f"""{{{{
    config(
        enabled=true,
        materialized="view",
        tags=["hydra_v2"],
    )
}}}}

select
{select_body}
from {{{{ source("hydra_v2", "{source_table}") }}}}
"""


def _stg_hyd_v2_model_yml(table: str) -> str:
    return f"""version: 2

models:
  - name: stg_hyd_v2_{table}
"""


def _hyd_v2_sources_yml(table_names: list[str]) -> str:
    lines = [
        "version: 2",
        "",
        "sources:",
        "  - name: hydra_v2",
        "    database: dwh_project",
        "    schema: dwh_trusted_staging",
        "    description: >",
        "      Hydra v2 raw tables from Cloud SQL CSV export.",
        "      String columns may contain <br /> newline placeholders.",
        "    tables:",
    ]
    for table in table_names:
        lines.append(f"      - name: hyd_v2_{table}")
    lines.append("")
    return "\n".join(lines)


def _hyd_v2_snapshot_sql(table: str) -> str:
    snapshot_name = f"hyd_v2_{table}_snapshot"
    stg_ref = f"stg_hyd_v2_{table}"
    uk = _dbt_snapshot_unique_key_config(table)
    return f"""{{% snapshot {snapshot_name} %}}

    {{{{
        config(
            {uk},
            strategy="check",
            check_cols="all",
            tags=["hydra_v2"],
            hard_deletes="invalidate",
        )
    }}}}

    select * from {{{{ ref("{stg_ref}") }}}}

{{% endsnapshot %}}
"""


def write_stg_hyd_v2_dbt_files(destination: Path | str) -> int:
    dest = Path(destination)
    dest.mkdir(parents=True, exist_ok=True)
    table_names: list[str] = []
    for spec in HYDRA_RAW_TABLES:
        table_names.append(spec.name)
        (dest / f"stg_hyd_v2_{spec.name}.sql").write_text(
            _stg_hyd_v2_model_sql(spec), encoding="utf-8"
        )
        (dest / f"stg_hyd_v2_{spec.name}.yml").write_text(
            _stg_hyd_v2_model_yml(spec.name), encoding="utf-8"
        )
    (dest / "_hyd_v2_sources.yml").write_text(
        _hyd_v2_sources_yml(table_names), encoding="utf-8"
    )
    print(f"wrote {len(table_names)} staging models to {dest}")
    return len(table_names)


def write_hyd_v2_snapshot_dbt_files(destination: Path | str) -> int:
    dest = Path(destination)
    dest.mkdir(parents=True, exist_ok=True)
    for spec in HYDRA_RAW_TABLES:
        path = dest / f"hyd_v2_{spec.name}_snapshot.sql"
        path.write_text(_hyd_v2_snapshot_sql(spec.name), encoding="utf-8")
        print("wrote", path)
    if HYDRA_RAW_TABLES_WITHOUT_PRIMARY_KEY:
        print(
            "snapshot unique_key uses all export columns (no MySQL PK):",
            ", ".join(HYDRA_RAW_TABLES_WITHOUT_PRIMARY_KEY),
        )
    return len(HYDRA_RAW_TABLES)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Hydra v2 utilities: BQ CSV schemas, dbt staging, snapshots.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_schemas = sub.add_parser("schemas", help="Write hyd_v2_<table>.json schemas")
    p_schemas.add_argument("destination", type=Path)

    p_stg = sub.add_parser("stg-dbt", help="Write stg_hyd_v2_* dbt models")
    p_stg.add_argument("destination", type=Path)

    p_snap = sub.add_parser("snapshots-dbt", help="Write hyd_v2_*_snapshot.sql")
    p_snap.add_argument("destination", type=Path)

    args = parser.parse_args()
    if args.command == "schemas":
        write_hyd_v2_schema_json_files(args.destination)
    elif args.command == "stg-dbt":
        write_stg_hyd_v2_dbt_files(args.destination)
    elif args.command == "snapshots-dbt":
        write_hyd_v2_snapshot_dbt_files(args.destination)
    else:
        raise SystemExit(f"unknown command: {args.command}")
