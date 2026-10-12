"""User Management Cloud SQL export catalog (trimmed).

Production landed ~28 tables from the identity MySQL database. This
module keeps eight representative tables that cover the engineering
contract: EU-style surrogate keys, MD5 ``_keyhash`` / ``_rowhash`` at
export time, and inline BigQuery schema fields for semicolon CSV loads.

Omitted from this reference (still in production):
- OAuth2 authorization / registered-client tables (token + secret material)
- REST API user password columns
- Spring Session attribute blobs
- employee invitation / mobile verification PII-heavy arms

Source (read-only):
  dags/etl_user_management.py  (inline table_schemas + tablenames)
"""

from __future__ import annotations

from typing import Any

# Production used concat('EU_', id) as a cross-system surrogate.
# Kept as UM_ here so the namespacing idea stays without the original tag.
SK_PREFIX = "UM_"

SOURCE_SYSTEM = "UserManagement"
JOB_NAME = "UserManagement.py"

ETL_TAIL = [
    {"name": "_create_ts", "type": "TIMESTAMP"},
    {"name": "_update_ts", "type": "TIMESTAMP"},
    {"name": "_job_name", "type": "STRING"},
    {"name": "_job_id", "type": "INTEGER"},
    {"name": "_sourcesystem", "type": "STRING"},
    {"name": "_keyhash", "type": "STRING"},
    {"name": "_rowhash", "type": "STRING"},
]


def _schema(*fields: dict[str, str]) -> list[dict[str, str]]:
    return list(fields) + list(ETL_TAIL)


TABLE_SCHEMAS: dict[str, list[dict[str, str]]] = {
    "applications": _schema(
        {"name": "id_sk", "type": "STRING"},
        {"name": "id", "type": "STRING"},
        {"name": "code", "type": "STRING"},
    ),
    "features": _schema(
        {"name": "id_sk", "type": "STRING"},
        {"name": "id", "type": "STRING"},
        {"name": "code", "type": "STRING"},
        {"name": "application_id_sk", "type": "STRING"},
        {"name": "application_id", "type": "STRING"},
    ),
    "locks": _schema(
        {"name": "name", "type": "STRING"},
    ),
    "personas": _schema(
        {"name": "id_sk", "type": "STRING"},
        {"name": "id", "type": "STRING"},
        {"name": "code", "type": "STRING"},
        {"name": "enabled", "type": "STRING"},
        {"name": "hierarchical_sort_position", "type": "STRING"},
        {"name": "sort_position", "type": "STRING"},
        {"name": "parent_persona_id_sk", "type": "STRING"},
        {"name": "parent_persona_id", "type": "STRING"},
    ),
    "personaassignments": _schema(
        {"name": "id_sk", "type": "STRING"},
        {"name": "id", "type": "STRING"},
        {"name": "end_date", "type": "STRING"},
        {"name": "establishment_id_sk", "type": "STRING"},
        {"name": "establishment_id", "type": "STRING"},
        {"name": "start_date", "type": "STRING"},
        {"name": "user_id_sk", "type": "STRING"},
        {"name": "user_id", "type": "STRING"},
        {"name": "persona_id_sk", "type": "STRING"},
        {"name": "persona_id", "type": "STRING"},
    ),
    "roleassignments": _schema(
        {"name": "id_sk", "type": "STRING"},
        {"name": "id", "type": "STRING"},
        {"name": "establishment_id_sk", "type": "STRING"},
        {"name": "establishment_id", "type": "STRING"},
        {"name": "user_id_sk", "type": "STRING"},
        {"name": "user_id", "type": "STRING"},
        {"name": "role_id_sk", "type": "STRING"},
        {"name": "role_id", "type": "STRING"},
    ),
    "roles": _schema(
        {"name": "id_sk", "type": "STRING"},
        {"name": "id", "type": "STRING"},
        {"name": "code", "type": "STRING"},
        {"name": "sort_position", "type": "STRING"},
        {"name": "initialapplication_id_sk", "type": "STRING"},
        {"name": "initialapplication_id", "type": "STRING"},
    ),
    "users": _schema(
        {"name": "id_sk", "type": "STRING"},
        {"name": "id", "type": "STRING"},
        {"name": "email", "type": "STRING"},
        {"name": "first_name", "type": "STRING"},
        {"name": "last_name", "type": "STRING"},
        {"name": "uuid", "type": "STRING"},
        {"name": "username", "type": "STRING"},
        {"name": "phone_number", "type": "STRING"},
        {"name": "language", "type": "STRING"},
        {"name": "last_external_modification_date", "type": "STRING"},
        {"name": "tc_date", "type": "STRING"},
        {"name": "tc_version", "type": "STRING"},
        {"name": "contact_id_sk", "type": "STRING"},
        {"name": "contact_id", "type": "STRING"},
        {"name": "country_code", "type": "STRING"},
        {"name": "creation_date", "type": "STRING"},
        {"name": "modification_date", "type": "STRING"},
        {"name": "salutation", "type": "STRING"},
    ),
}


def _sk(expr: str) -> str:
    return f"CONCAT('{SK_PREFIX}', CAST({expr} AS char))"


def _etl_literals() -> str:
    return (
        f"current_timestamp AS _create_ts, "
        f"'' AS _update_ts, "
        f"'{JOB_NAME}' AS _job_name, "
        f"0 AS _job_id, "
        f"'{SOURCE_SYSTEM}' AS _sourcesystem"
    )


EXPORT_QUERIES: dict[str, str] = {
    "applications": f"""
        SELECT
            {_sk('id')} AS id_sk,
            id,
            IFNULL(code, ''),
            {_etl_literals()},
            md5(CAST(id AS char)) AS _keyhash,
            md5(CONCAT(IFNULL(CAST(id AS char), ''), '|', IFNULL(CAST(code AS char), ''))) AS _rowhash
        FROM applications
    """,
    "features": f"""
        SELECT
            {_sk('id')} AS id_sk,
            id,
            IFNULL(code, ''),
            {_sk("IFNULL(application_id, '')")} AS application_id_sk,
            IFNULL(application_id, ''),
            {_etl_literals()},
            md5(CAST(id AS char)) AS _keyhash,
            md5(CONCAT(
                IFNULL(CAST(id AS char), ''), '|',
                IFNULL(CAST(code AS char), ''), '|',
                IFNULL(CAST(application_id AS char), '')
            )) AS _rowhash
        FROM features
    """,
    "locks": f"""
        SELECT
            IFNULL(name, ''),
            {_etl_literals()},
            md5(CAST(name AS char)) AS _keyhash,
            md5(IFNULL(CAST(name AS char), '')) AS _rowhash
        FROM locks
    """,
    "personas": f"""
        SELECT
            {_sk('id')} AS id_sk,
            id,
            IFNULL(code, ''),
            IFNULL(enabled, ''),
            IFNULL(hierarchical_sort_position, ''),
            IFNULL(sort_position, ''),
            {_sk("IFNULL(parent_persona_id, '')")} AS parent_persona_id_sk,
            IFNULL(parent_persona_id, ''),
            {_etl_literals()},
            md5(CAST(id AS char)) AS _keyhash,
            md5(CONCAT(
                IFNULL(CAST(id AS char), ''), '|',
                IFNULL(CAST(code AS char), ''), '|',
                IFNULL(CAST(enabled AS char), ''), '|',
                IFNULL(CAST(hierarchical_sort_position AS char), ''), '|',
                IFNULL(CAST(sort_position AS char), ''), '|',
                IFNULL(CAST(parent_persona_id AS char), '')
            )) AS _rowhash
        FROM personas
    """,
    "personaassignments": f"""
        SELECT
            {_sk('id')} AS id_sk,
            id,
            IFNULL(end_date, ''),
            {_sk("IFNULL(establishment_id, '')")} AS establishment_id_sk,
            IFNULL(establishment_id, ''),
            IFNULL(start_date, ''),
            {_sk("IFNULL(user_id, '')")} AS user_id_sk,
            IFNULL(user_id, ''),
            {_sk("IFNULL(persona_id, '')")} AS persona_id_sk,
            IFNULL(persona_id, ''),
            {_etl_literals()},
            md5(CAST(id AS char)) AS _keyhash,
            md5(CONCAT(
                IFNULL(CAST(id AS char), ''), '|',
                IFNULL(CAST(end_date AS char), ''), '|',
                IFNULL(CAST(establishment_id AS char), ''), '|',
                IFNULL(CAST(start_date AS char), ''), '|',
                IFNULL(CAST(user_id AS char), ''), '|',
                IFNULL(CAST(persona_id AS char), '')
            )) AS _rowhash
        FROM personaassignments
    """,
    "roleassignments": f"""
        SELECT
            {_sk('id')} AS id_sk,
            id,
            {_sk("IFNULL(establishment_id, '')")} AS establishment_id_sk,
            IFNULL(establishment_id, ''),
            {_sk("IFNULL(user_id, '')")} AS user_id_sk,
            IFNULL(user_id, ''),
            {_sk("IFNULL(role_id, '')")} AS role_id_sk,
            IFNULL(role_id, ''),
            {_etl_literals()},
            md5(CAST(id AS char)) AS _keyhash,
            md5(CONCAT(
                IFNULL(CAST(id AS char), ''), '|',
                IFNULL(CAST(establishment_id AS char), ''), '|',
                IFNULL(CAST(user_id AS char), ''), '|',
                IFNULL(CAST(role_id AS char), '')
            )) AS _rowhash
        FROM roleassignments
    """,
    "roles": f"""
        SELECT
            {_sk('id')} AS id_sk,
            id,
            IFNULL(code, ''),
            IFNULL(sort_position, ''),
            {_sk("IFNULL(initialapplication_id, '')")} AS initialapplication_id_sk,
            IFNULL(initialapplication_id, ''),
            {_etl_literals()},
            md5(CAST(id AS char)) AS _keyhash,
            md5(CONCAT(
                IFNULL(CAST(id AS char), ''), '|',
                IFNULL(CAST(code AS char), ''), '|',
                IFNULL(CAST(sort_position AS char), ''), '|',
                IFNULL(CAST(initialapplication_id AS char), '')
            )) AS _rowhash
        FROM roles
    """,
    "users": f"""
        SELECT
            {_sk('id')} AS id_sk,
            id,
            IFNULL(email, ''),
            IFNULL(first_name, ''),
            IFNULL(last_name, ''),
            IFNULL(uuid, ''),
            IFNULL(username, ''),
            IFNULL(phone_number, ''),
            IFNULL(language, ''),
            IFNULL(last_external_modification_date, ''),
            IFNULL(tc_date, ''),
            IFNULL(tc_version, ''),
            {_sk("IFNULL(contact_id, '')")} AS contact_id_sk,
            IFNULL(contact_id, ''),
            IFNULL(country_code, ''),
            IFNULL(creation_date, ''),
            IFNULL(modification_date, ''),
            IFNULL(salutation, ''),
            {_etl_literals()},
            md5(CAST(id AS char)) AS _keyhash,
            md5(CONCAT(
                IFNULL(CAST(id AS char), ''), '|',
                IFNULL(CAST(email AS char), ''), '|',
                IFNULL(CAST(first_name AS char), ''), '|',
                IFNULL(CAST(last_name AS char), ''), '|',
                IFNULL(CAST(uuid AS char), ''), '|',
                IFNULL(CAST(username AS char), ''), '|',
                IFNULL(CAST(phone_number AS char), ''), '|',
                IFNULL(CAST(language AS char), ''), '|',
                IFNULL(CAST(last_external_modification_date AS char), ''), '|',
                IFNULL(CAST(tc_date AS char), ''), '|',
                IFNULL(CAST(tc_version AS char), ''), '|',
                IFNULL(CAST(contact_id AS char), ''), '|',
                IFNULL(CAST(country_code AS char), ''), '|',
                IFNULL(CAST(creation_date AS char), ''), '|',
                IFNULL(CAST(modification_date AS char), ''), '|',
                IFNULL(CAST(salutation AS char), '')
            )) AS _rowhash
        FROM users
    """,
}

TABLE_NAMES: list[str] = list(EXPORT_QUERIES.keys())

assert set(TABLE_NAMES) == set(TABLE_SCHEMAS.keys())
assert len(TABLE_NAMES) == 8


def schema_fields(table: str) -> list[dict[str, Any]]:
    return TABLE_SCHEMAS[table]
