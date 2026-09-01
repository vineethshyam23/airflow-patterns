# Data flow: Maileon email marketing import

## Schedule

- PROD: `0 2 * * *` (02:00 UTC)
- DEV: manual (`schedule=None`)

## Report grains

| Report | Endpoint | Staging table |
|--------|----------|---------------|
| opens | `/reports/opens` | `maileon_opens` |
| opens_unique | `/reports/opens/unique` | `maileon_opens_unique` |
| clicks | `/reports/clicks` | `maileon_clicks` |
| clicks_unique | `/reports/clicks/unique` | `maileon_clicks_unique` |
| bounces | `/reports/bounces` | `maileon_bounces` |
| blocks | `/reports/blocks` | `maileon_blocks` |
| unsubscriptions | `/reports/unsubscriptions` | `maileon_unsubscriptions` |
| recipients | `/reports/recipients` | `maileon_recipients` |

## Steps

1. **Extract (×8, parallel)** — `import_maileon_data` pages the
   report, writes
   `data/maileon/{report}/{report}_{YYYYMMDD}.jsonl` on Composer.
2. **Branch** — blob exists and `size > 0` → copy path; else skip.
3. **Copy** — Composer object → `rawzone/maileon/{date}/{report}.jsonl`.
4. **Load** — NEWLINE_DELIMITED_JSON into
   `trusted_staging.maileon_{report}` with explicit schema JSON,
   WRITE_TRUNCATE. Trigger rule joins copy and skip.
5. **dbt transform** — builds `trusted.int_maileon_*` (and related).
6. **Names** — query distinct mailing_ids from int tables → per-id
   `/mailings/{id}/name` → NDJSON →
   `trusted_staging.maileon_names_tbl`.
7. **Tags** — mailing_ids from names staging →
   `/mailings/{id}/settings/tags` → NDJSON →
   `trusted_staging.maileon_tags_tbl`.
8. **stage** — ALL_DONE barrier.
9. **dbt names / dbt API** — fold metadata; API job uses ALL_DONE so
   it still runs when an upstream soft-fails.

## Empty-file behavior

| Case | Branch | Load effect |
|------|--------|-------------|
| JSONL size > 0 | copy → load | Truncate + load new file |
| Empty or missing | skip → load | Load still runs; may truncate against missing/stale object |

Production kept the join for a single downstream dbt dependency.
Safer redesign: skip path ends the branch; dbt waits only on
successful loads (or uses a soft sensor).

## Failure modes

- **401/403 on extract** — fail the report branch; retries (2× / 10m)
- **429/500 on name/tag** — backoff inside the enrichment callables
- **Missing schema JSON** — BigQuery load fails; fix schema object path
- **Parse-time dates** — delayed DAG parse can stamp the wrong
  `YYYYMMDD` into object names; align with `ds` when hardening
- **PII** — recipients include emails; restrict dataset IAM

## Re-run

Re-triggering the DAG re-pulls all eight reports and TRUNCATEs
staging. Safe for idempotent daily refresh; not a partial replay
tool for a single report without surgically clearing task instances.
