# Data flow: User Management Cloud SQL SCD Type 2

## Schedule

| Setting | Value |
|---------|-------|
| Cron | `0 7 * * *` (07:00 UTC) |
| `max_active_runs` | 1 |
| `dagrun_timeout` | 300 minutes |
| Cloud SQL backup window | 03:00–05:00 UTC (export delay doubled) |

## Per-table chain (×8 in this reference)

1. `_echo_{table}` — log breadcrumb (`TriggerRule.ALL_DONE` from prior export)
2. `sql_export_{table}` — Cloud SQL Admin CSV (`fieldsTerminatedBy: 3B`)
3. `download_file_*` — export bucket → raw zone (same object path)
4. `load_{table}` — semicolon CSV → `trusted_staging.stg_um_{table}` WRITE_TRUNCATE
5. `copy_table_{table}_stg` — bootstrap trusted if missing
6. `copy_table_{table}_tmp` — snapshot trusted → `tmp_um_{table}`
7. `update_{table}` — expire open rows whose hash is absent from staging
8. `insert_{table}` — append staging rows whose hash is new vs open trusted
9. `copy_table_{table}` — promote tmp → `trusted.um_{table}` WRITE_TRUNCATE

Exports are wired serially: finish table N export before starting table
N+1 echo. SCD chains for earlier tables can still be running while later
exports proceed.

## Object layout

```
gs://{export_bucket}/{db}/{table}/{ds}/000000/{table}.csv
gs://{raw_bucket}/{db}/{table}/{ds}/000000/{table}.csv
```

Production used parse-time `date.today()` for the date folder. This
reference uses `{{ ds }}` so manual clear / backfill does not land on
the wrong day.

## Grain and SCD contract

| Column | Role |
|--------|------|
| `id_sk` / `*_sk` | Namespaced surrogate (`UM_` + source id) |
| `_keyhash` | MD5 of business key (usually id) |
| `_rowhash` | MD5 of concatenated compared columns |
| `_valid_flag` | True while row is current |
| `_valid_from` / `_valid_until` | Hour-truncateded validity window |

Expire sets `_valid_until` to one second before the current hour bucket
and clears `_valid_flag`. Insert stamps `_valid_from` to that hour and
`_valid_until` to `2099-12-31`.

## CSV contract

- Delimiter `;`, quote `"`, header skipped
- `allow_quoted_newlines` / `allow_jagged_rows` / `ignore_unknown_values`
- `max_bad_records=100`
- Schema from `TABLE_SCHEMAS` (not external JSON)

## Tables in this reference

| Domain | Tables |
|--------|--------|
| Users & auth | `users`, `roles`, `roleassignments` |
| Personas | `personas`, `personaassignments` |
| Catalog | `applications`, `features` |
| System | `locks` |

## Downstream

Warehouse streams and reverse-ETL consumers read `trusted.um_*` open
rows (`_valid_flag=True`). This pattern does not ship those consumers.
