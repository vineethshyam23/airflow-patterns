# Business case: Reservation Tool incremental Cloud SQL export

## Problem

The reservation product OLTP database is Cloud SQL MySQL. Fact tables
(`reservations`, `customers`, SMS / RWG action logs) grew into tens of
GB. The legacy DAG exported every table every night as a full CSV.
That burned Cloud SQL Admin time, filled the raw bucket with duplicate
bytes, and routinely collided with the 03:00 UTC backup window —
409 `operationInProgress` failures that woke people up.

Transformations were worse: PII masking, surrogate keys, and SCD Type 2
lived as inline BigQuery SQL inside the DAG. Schema drift meant editing
the orchestrator. Restricted / confidential customer and establishment
variants were separate Cloud SQL exports, doubling the largest tables.

## Decision

Split responsibilities and shrink the daily export:

1. **Airflow owns raw landing only** — Cloud SQL Admin CSV → GCS →
   BigQuery staging (`rt_<table>`).
2. **Watermark the append-heavy tables** — before export, read
   `MAX(id)` from staging; export `WHERE id > max_id` with
   WRITE_APPEND. Tables without a usable auto_increment watermark
   stay on full WRITE_TRUNCATE.
3. **Sunday full baseline** — force `max_id = 0` and truncate
   incremental staging so the weekly run is a clean reload. Delete
   detection stays in dbt snapshots, not in the export SQL.
4. **dbt owns masking and historization** — one Cloud job builds
   staging models, SCD snapshots, and restricted / confidential
   views from the raw tables. No second export for "sensitive"
   variants.
5. **Reuse the schedule-aware export operator** — same 409 retry +
   backup-window backoff family as Hydra (pattern 39).

Daily Cloud SQL export volume dropped from roughly full-schema size
to the overnight delta on the large tables. The operability win is
fewer 409 storms and a reviewable catalog instead of a
multi-thousand-line DAG.

## Constraints I cared about

- Cloud SQL: one long-running Admin op per instance. Serialize
  exports; never fan out exports in parallel. Loads can overlap.
- CSV known issues: NULL becomes `\N`, `bit(1)` can emit NUL bytes,
  free-text newlines break quote balance. Fix in the SELECT
  (`IFNULL`, CAST UNSIGNED, REPLACE newlines/quotes), not after a
  bad load.
- Security: password / reset-token / Facebook access token columns
  never leave MySQL. Guest PII still lands in staging raw; dbt
  masks before anything leaves the trusted layer.
- `max_active_runs=1` — overlapping watermark reads would race.

## What I would not claim

No invented euro savings or headcount. The measurable change is
export volume and on-call noise. Watermarking only works when the
source really is append-mostly on auto_increment ids — CMS-style
soft-delete schemas (Hydra) stay on full dump + dbt snapshot.
