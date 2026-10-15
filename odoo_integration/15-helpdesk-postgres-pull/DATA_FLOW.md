# Data flow: Odoo helpdesk Postgres pull

Schedule in source: `schedule_interval=None` (manual / externally
triggered). Catchup off. `max_active_runs=1`.

## Stage A — Extract to Composer data volume

| Table | Window | Notes |
|-------|--------|-------|
| `helpdesk_ticket` | create_date or write_date in today/yesterday | Incremental; portal `access_token` omitted |
| `helpdesk_team` | full | Includes support phone / hours custom fields |
| `helpdesk_ticket_type` | full | |
| `helpdesk_ticket_medium` | full | |
| `helpdesk_stage` | full | |
| `helpdesk_tag` | full | |
| `helpdesk_tag_helpdesk_ticket_rel` | full | Junction; truncate+reload |
| `mail_message` (optional) | create/write in today/yesterday for `helpdesk.ticket` / `account.payment` | Off by default |

Each fetch writes `{tmp_loc}{table}.json` as newline-delimited JSON.
Default `tmp_loc` is `/home/airflow/gcs/data/odoo/` on Composer.

## Stage B — Raw zone copy

`GCSToGCSOperator` copies from the Composer bucket object
`data/odoo/{table}.json` into
`{rawzone}/odoo/{table}/{YYYY-MM-DD}/`. Load date is `date.today()` at
DAG parse/run — fine for on-demand; if you add a cron, prefer
`{{ ds }}` so catchup-style re-runs land under the logical date.

## Stage C — Staging load

`GCSToBigQueryOperator` loads NDJSON into `staging.odoo_{table}` with
WRITE_TRUNCATE and schema from `schema_json/odoo_{table}.json`. Source
had a crm_lead-style APPEND branch that does not apply to this table
list — every default table truncates.

## Stage D — dbt refresh

One `DbtCloudRunJobOperator` after all loads. Trusted helpdesk models
(and anything pattern 06 reads) rebuild from staging. If dbt fails,
staging still holds the latest pull — re-run dbt without re-hitting
Odoo when the ERP was the expensive part.

## Idempotency and re-runs

- Re-trigger: re-pulls tickets for the rolling two-day window and
  re-truncates dims. Dated raw paths from the same calendar day
  overwrite the same prefix when LOAD_DATE is today.
- Partial failure mid-chain: `end` uses ALL_DONE so the run closes;
  clear and re-run from the failed table task. Earlier tables already
  truncated staging — re-running only the failed triple is usually
  enough.
- Do not enable `mail_message` casually. Bodies are large and often
  contain customer PII; treat as a separate compliance decision.

## Failure modes worth knowing

- Odoo SSL / wait-callback hangs: production disabled the psycopg2 wait
  callback for a reason — leave that alone.
- Empty ticket file: possible on quiet days; dims should never be empty
  in a live ERP. Empty dim → check credentials / wrong database.
- Schema JSON missing in the raw bucket: BQ load fails before dbt.
  Keep schema objects versioned next to the landing path.
- Sequential chain length: seven tables × three tasks + dbt. A stuck
  early fetch blocks everything behind it — that is the operability
  tradeoff of the source graph.
