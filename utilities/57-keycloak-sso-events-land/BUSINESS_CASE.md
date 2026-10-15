# Business Case: Keycloak SSO events land

Security, product, and support all needed a warehouse copy of Keycloak
login and session events — not another trip into the identity team's
backup bucket when an incident happened. The identity platform already
exported a daily Keycloak events tarball into a backup GCS bucket. The
DWH job's job was to make that file queryable in trusted with the same
lineage columns every other fact table carries.

I kept the unpack on the Composer worker (GCS FUSE `data/` mount +
`tar -xzf`) rather than a Cloud Function or a direct BigQuery load of
the tarball for three production reasons:

1. **The backup contract was a tar.gz, not a CSV.** BigQuery's load
   job wants a delimited file. Unpacking once on the worker and
   republishing a plain CSV into the rawzone kept the load path
   identical to every other CSV land we already ran.
2. **IAM boundary between product backup and DWH rawzone.** The
   identity backup bucket is owned by the platform that runs Keycloak.
   Composer already has write on the DWH rawzone. Copying through the
   Composer data prefix is the same dual-concern pattern we used for
   product→rawzone lands elsewhere — cheaper than negotiating a
   permanent cross-team write grant on the backup bucket.
3. **Operational familiarity.** Six sequential GCS / Bash / BQ tasks
   are boring. Boring is what you want at 07:00 UTC for an
   authentication audit table.

## What this unlocks

- Trusted `sso_events` for login analytics, client/realm breakdowns,
  and incident timelines.
- `details_json` preserved as a string so Keycloak payload changes do
  not break the load schema.
- Standard DWH metadata (`_create_ts`, `_valid_from` / `_valid_until`,
  `_sourcesystem='SSO'`) so SSO rows join the rest of the trusted
  model without a special case.

## Tradeoffs I accepted

- **Append-only, no dedupe.** Re-running the DAG on the same calendar
  day re-appends staging into trusted. Event `id` can duplicate. We
  accepted that because the backup object is one file per day and
  ops rarely re-ran; a MERGE / `WHERE id NOT IN (...)` would be the
  first hardening step.
- **`retries=0`.** The original graph failed loud and stopped. That
  is harsh for a six-hop chain (GCS copy + tar + GCS + BQ load +
  insert). I left it visible in the sample so the portfolio does not
  pretend production was softer than it was.
- **Parse-time `loaddate`.** Yesterday is computed when the DAG is
  parsed, not from `{{ ds }}`. Fine while the scheduler parses daily
  and nobody backfills. Wrong the moment you re-parse mid-day or
  run a historical clear. Template with `macros.ds_add(ds, -1)` if
  you rebuild.
- **`max_bad_records=100`.** Keycloak CSV exports occasionally
  ship a jagged details field. Failing the whole day on a handful of
  rows burned more SLA than the bad-row budget.

## Not this pattern

- Pattern 49 / 51: Adobe Analytics Data Feed unpack (lookups +
  refined enrich, different vendor contract).
- Pattern 30: Medallia SCD Type 2 (hash history, not append-only).
- Shim-manager access-log parser (`sso_manager_events.py`) — that
  turns HTTP access logs into NDJSON and is a separate utility, not
  the Keycloak events land.
