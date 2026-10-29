"""
SQL builders for First Best Offer score historization (SCD Type 2).

Two score populations share the same contract:

1. Stage: project today's score table into staging with audit + hash columns.
2. Expire: close currently-valid hist rows whose keyhash appears in staging
   but whose keyhash|rowhash pair does not (content changed).
3. Insert: append staging rows whose keyhash|rowhash is not already a
   currently-valid hist row.

Key grain differs by population:
  - wholesale (partner-account) scores key on wholesale_account_id
  - independent establishment scores key on establishment_id

Source tables are written by the VM predict step (out of this module).
"""

from __future__ import annotations

from dataclasses import dataclass


FAR_FUTURE = 'timestamp("2099-12-31 00:00:00")'
JOB_NAME = "etl_first_best_offer"
SOURCE_SYSTEM = "scoring"


@dataclass(frozen=True)
class FboScoreArm:
    """One score population: source score table → staging → historized."""

    name: str
    key_column: str
    source_table: str
    staging_table: str
    hist_table: str


WHOLESALE = FboScoreArm(
    name="wholesale",
    key_column="wholesale_account_id",
    source_table="fbo_wholesale_score",
    staging_table="tmp_fbo_wholesale_score",
    hist_table="fbo_wholesale_score_historized",
)

INDEPENDENT = FboScoreArm(
    name="independent",
    key_column="establishment_id",
    source_table="fbo_independent_score",
    staging_table="tmp_fbo_independent_score",
    hist_table="fbo_independent_score_historized",
)

ARMS = (WHOLESALE, INDEPENDENT)


def stage_sql(
    project: str,
    refined_dataset: str,
    staging_dataset: str,
    arm: FboScoreArm,
) -> str:
    """Build SELECT that materializes staging with SCD audit columns."""
    source = f"`{project}.{refined_dataset}.{arm.source_table}`"
    return f"""
WITH scored AS (
  SELECT
    *,
    CURRENT_TIMESTAMP() AS _create_ts,
    CAST(NULL AS TIMESTAMP) AS _update_ts,
    '{JOB_NAME}' AS _job_name,
    0 AS _job_id,
    '{SOURCE_SYSTEM}' AS _sourcesystem,
    TO_HEX(MD5(CAST({arm.key_column} AS STRING))) AS _keyhash,
    TO_HEX(MD5(TO_JSON_STRING(t))) AS _rowhash,
    TIMESTAMP(CURRENT_DATE()) AS _valid_from,
    {FAR_FUTURE} AS _valid_until,
    TRUE AS _valid_flag
  FROM {source} AS t
)
SELECT * FROM scored
""".strip()


def expire_sql(
    project: str,
    refined_dataset: str,
    staging_dataset: str,
    arm: FboScoreArm,
) -> str:
    """Close hist rows whose content changed for a known keyhash."""
    staging = f"`{project}.{staging_dataset}.{arm.staging_table}`"
    hist = f"`{project}.{refined_dataset}.{arm.hist_table}`"
    return f"""
UPDATE {hist}
SET
  _valid_until = TIMESTAMP(
    FORMAT_TIMESTAMP(
      "%Y-%m-%d 23:59:59",
      TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
    )
  ),
  _valid_flag = FALSE,
  _update_ts = CURRENT_TIMESTAMP()
WHERE _keyhash IN (SELECT _keyhash FROM {staging})
  AND CONCAT(_keyhash, _rowhash) NOT IN (
    SELECT CONCAT(_keyhash, _rowhash) FROM {staging}
  )
""".strip()


def insert_sql(
    project: str,
    refined_dataset: str,
    staging_dataset: str,
    arm: FboScoreArm,
) -> str:
    """Select new or changed staging rows for WRITE_APPEND into hist."""
    staging = f"`{project}.{staging_dataset}.{arm.staging_table}`"
    hist = f"`{project}.{refined_dataset}.{arm.hist_table}`"
    return f"""
SELECT *
FROM {staging}
WHERE CONCAT(_keyhash, _rowhash) NOT IN (
  SELECT CONCAT(_keyhash, _rowhash)
  FROM {hist}
  WHERE _valid_flag = TRUE
)
""".strip()


if __name__ == "__main__":
    for arm in ARMS:
        print(f"--- {arm.name} stage ---")
        print(stage_sql("dwh_project", "refined", "trusted_staging", arm)[:180], "...")
        print(f"--- {arm.name} expire ---")
        print(expire_sql("dwh_project", "refined", "trusted_staging", arm)[:180], "...")
