"""Finance-kit entity registry and hash-delta INSERT SQL builder.

Source dump lands one CSV per entity. Staging is truncated each run;
trusted grows by (_keyhash, _rowhash) pairs not already present.
"""

from __future__ import annotations

# Vendor file stems: landingzone/finance_kit/{entity}_{date}.csv
ENTITIES = [
    "angaben",
    "bwa",
    "bwa_types",
    "doctrine_migration_versions",
    "eingabe",
    "import",
    "kosten",
    "project",
    "umsatz",
    "user",
]


def build_hash_delta_insert_sql(
    *,
    project_id: str,
    staging_dataset: str,
    trusted_dataset: str,
    entity: str,
    job_name: str = "etl_finance_kit.py",
    source_system: str = "FinanceKit",
) -> str:
    """Append rows whose key|row hash pair is new in trusted.

    Staging already carries _keyhash / _rowhash from the load schema.
    Audit columns are stamped here; validity window is open-ended
    (valid_until far future + valid_flag True). This is append-only
    hash dedupe, not a full SCD2 close of prior versions.
    """
    staging = f"`{project_id}.{staging_dataset}.finance_kit_{entity}`"
    trusted = f"`{project_id}.{trusted_dataset}.finance_kit_{entity}`"
    return f"""
SELECT
  * EXCEPT (_keyhash, _rowhash),
  CURRENT_TIMESTAMP() AS _create_ts,
  CAST(NULL AS TIMESTAMP) AS _update_ts,
  '{job_name}' AS _job_name,
  0 AS _job_id,
  '{source_system}' AS _sourcesystem,
  _keyhash,
  _rowhash,
  TIMESTAMP(CURRENT_DATE()) AS _valid_from,
  TIMESTAMP('2099-12-31 23:59:59') AS _valid_until,
  TRUE AS _valid_flag
FROM {staging}
WHERE CONCAT(_keyhash, '|', _rowhash) NOT IN (
  SELECT CONCAT(_keyhash, '|', _rowhash) FROM {trusted}
)
""".strip()
