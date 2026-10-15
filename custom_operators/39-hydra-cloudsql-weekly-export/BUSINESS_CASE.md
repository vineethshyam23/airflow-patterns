# Business case: Hydra Cloud SQL weekly full export

## Problem

The product website (CMS / establishment pages) lives in Cloud SQL
MySQL. Downstream analytics — activity scores, derived events, menu
widgets — need a trustworthy copy in BigQuery. The legacy Hydra DAG
grew into a multi-thousand-line file that mixed Cloud SQL export, ad
hoc column casting, and trusted-layer INSERT/UPDATE logic for dozens
of tables. Every schema drift meant editing SQL inside the DAG. Cloud
SQL also rejects concurrent Admin API operations, so parallel exports
failed with 409s while nightly backups ran.

## Decision

Split responsibilities:

1. **Airflow owns raw landing only** — Cloud SQL Admin CSV export →
   GCS → BigQuery staging (`hyd_v2_<table>`, WRITE_TRUNCATE).
2. **dbt owns historization** — staging views strip credentials;
   snapshots apply check-strategy SCD. No watermark in the export SQL.
3. **One custom operator** — retry `operationInProgress` and stretch
   backoff inside the known backup window so a 80-table serial chain
   survives the night.

Full weekly (or daily-scheduled) dumps look expensive until you count
the on-call cost of broken incremental keys on CMS tables that soft
delete, rename columns, and add bit flags without notice. Staging
TRUNCATE + dbt snapshot unique keys (PK or all columns) was the
cheaper operability tradeoff.

## Constraints I cared about

- Cloud SQL: one long-running op per instance. Serialize exports;
  never fan out exports in parallel.
- CSV known issues: NULL becomes `"N`, newlines break quote balance.
  Fix in SELECT (`IFNULL`, doubled quotes, `<br />` for CR/LF), not
  after a bad load.
- Security: restricted / confidential establishment tables and OAuth
  credential tables stay out of the active catalog until IAM + masking
  exist. Password columns on `users` never enter dbt staging.
- Migration: legacy `etl_hydra` kept only for the short derived-events
  allowlist; everything else moves to v2 so we do not dual-write forever.

## What I would not claim

No invented savings or team-size metrics. The win is fewer 409
failures, a reviewable table catalog, and schema/JSON/dbt stubs
generated from the same `HYDRA_RAW_TABLES` source of truth.
