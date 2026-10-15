# Pattern 57: Keycloak SSO events land

Daily Composer DAG that lands Keycloak authentication/event logs from
an identity-platform backup bucket into BigQuery trusted
``sso_events`` as an **append-only** fact.

The interesting engineering is not the SQL — it is the hop across a
Composer FUSE data mount: backup object → Composer ``data/sso/`` →
``tar -xzf`` on the worker → re-publish plain CSV into the DWH
rawzone → staging TRUNCATE → trusted APPEND with lineage columns and
``TIMESTAMP_MILLIS`` conversion.

Distinct from Adobe rawfeed (#49 / #51): those unpack vendor Data Feed
packs with lookup fan-out and refined enrich. This is a single CSV
tarball from the identity backup job, no dimension decode, no dedupe
window on append.

Source (read-only):
- `dags/etl_sso.py`

Supporting module `sso_manager_events.py` (shim access-log parser) is
a different concern and is not shipped here.

## Files

| File | Role |
|------|------|
| `dag_keycloak_sso_events.py` | Six-task land: copy → unpack → rawzone → stage → append |
| `trusted_append.sql` | Staging → trusted SELECT with metadata columns |
| `BUSINESS_CASE.md` | Why land identity events this way |
| `ARCHITECTURE.md` | Components + Mermaid diagram |
| `DATA_FLOW.md` | Object layout, parse-time date gotcha, failure modes |

Staging CSV columns (production schema object on rawzone
`schema_json/sso_events.json`): `id`, `client_id`, `details_json`,
`error`, `ip_address`, `realm_id`, `session_id`, `event_timestamp`
(epoch millis), `type`, `user_id`.

## Quick start

```bash
python -c "import ast; ast.parse(open('dag_keycloak_sso_events.py').read())"
python -c "from pathlib import Path; print(Path('trusted_append.sql').read_text()[:80])"
```

Needs Variables `composer_bucket`, `identity_backup_bucket`,
`rawzone_bucket`, `dwh_project_id`, plus schema JSON under
`schema_json/sso_events.json` on the rawzone bucket. This folder is a
sanitized reference, not a deploy.

## Sanitization notes

- Backup bucket `shim-manager-backups-prod` → Variable
  `identity_backup_bucket` / default `identity-backups`
- Rawzone `hd-digital-dp-rawzone` → Variable `rawzone_bucket` /
  `rawzone`
- Project `hd-dwh-stream-1` → Variable `dwh_project_id` /
  `dwh_project`
- Datasets `dwh_trusted*` → `trusted` / `trusted_staging`
- DAG id `etl_sso` → `etl_sso_events`; job name → `etl_sso_events`
- Owner / real emails → `data-platform` / `dataops@example.com`
- Unused `TriggerDagRunOperator` import dropped
- Deprecated `BashOperator` import path → `airflow.operators.bash`
- `max_active_runs=1` and tags added (production had neither)
- `retries=0` preserved and called out — that was a real ops pain

## Distinct from nearby patterns

| | 49 / 51 (Adobe) | 30 (Medallia SCD2) | 57 (this) |
|---|---|---|---|
| Source | Analytics Data Feed packs | Survey GraphQL / CSV | Keycloak backup tar.gz |
| Hard part | Dual unpack + lookup fan-out | Inline SCD2 | Composer FUSE unpack + append-only trusted |
| Idempotency | hit_id anti-join window | SCD hash | None — re-run duplicates |

## Category

`utilities/57-keycloak-sso-events-land/`
