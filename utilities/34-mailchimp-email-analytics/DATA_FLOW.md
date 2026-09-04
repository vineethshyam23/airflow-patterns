# Data flow: Mailchimp email analytics ingest

## Schedule

- PROD: `0 1 * * *` (01:00 UTC)
- DEV: manual (`schedule=None`)

## Entity grains

| Entity | API surface | Staging / trusted table |
|--------|-------------|-------------------------|
| campaign_list | `campaigns.list` (paginated) | `mailchimp_campaign_list` |
| campaign_reports | `reports.get_campaign_report` | `mailchimp_campaign_reports` |
| click_report | `reports.get_campaign_click_details` | `mailchimp_click_report` |
| unsubscribes | `reports.get_unsubscribed_list_for_campaign` | `mailchimp_unsubscribes` |
| email_activity | `reports.get_email_activity_for_campaign` | `mailchimp_email_activity` |
| recipients | `reports.get_campaign_recipients` | `mailchimp_recipients` |

## Steps

1. **campaign_list_fetch** — page `/campaigns`, write
   `data/mailchimp/campaign_list/campaign_list.json`.
2. **campaign_report_fetch … recipients_fetch** — for each campaign
   ID from `trusted_staging.mailchimp_campaign_list` (last 90 days
   by `sent_time`), call the report endpoint with up to 10 attempts;
   write one JSONL file per campaign under
   `data/mailchimp/{entity}/`.
3. **pause** — ALL_DONE barrier after all extracts.
4. **upload_storage_{entity}** (×6, parallel) — Composer bucket
   `data/mailchimp/{entity}/*.json` →
   `rawzone/mailchimp/{entity}/{loaddate}/`.
5. **load_staging_{entity}** — NEWLINE_DELIMITED_JSON into
   `trusted_staging.mailchimp_{entity}` with schema object
   `schema_json/{entity}.json`, day partitioning, WRITE_APPEND.
6. **copy_table_to_trusted_*** — BigQuery table copy staging →
   `trusted.mailchimp_{entity}` with WRITE_TRUNCATE.
7. **end** — joins all six land chains.

## Campaign-ID dependency

Report extractors read staging, not the JSONL just written in step 1.
Same-day report fan-out therefore relies on campaign rows already
present from prior runs (or a separate load of today's list before
report tasks). The 90-day window makes this workable in practice;
hardening options:

- Load campaign_list into staging before report tasks in the same
  DAG (insert a mini land chain after `campaign_list_fetch`)
- Or pass today's campaign IDs via XCom / a temp table

## Failure modes

| Case | Effect |
|------|--------|
| Bad API key / server prefix | Extract tasks fail; Airflow retries 2× / 10m |
| Single campaign timeout | Up to 10 local attempts; grain continues with other IDs |
| Missing schema JSON | GCSToBigQuery fails for that entity only |
| Empty campaign window | Report files empty/minimal; APPEND still runs |
| Parse-time `loaddate` drift | Object prefix may not match intended `ds` |

## PII

`recipients`, `unsubscribes`, and `email_activity` include email
addresses and CRM merge fields. Restrict dataset IAM and retention
accordingly.

## Re-run

Re-triggering re-pulls all six grains and APPENDs another day into
staging, then TRUNCATEs trusted from the full staging table. Safe
for daily refresh; not a surgical single-entity replay without
clearing task instances and managing duplicate `load_date` rows in
staging.
