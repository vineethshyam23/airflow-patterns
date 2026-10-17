# Data flow: Mailchimp campaign analytics integration

Schedule: `0 1 * * *` (01:00 UTC daily). Catchup off — we want today's
extract, not a historical replay.

## Stage A — Sequential API fetch

All fetch tasks write JSONL to Composer local paths under
`/home/airflow/gcs/data/mailchimp/{entity}/`.

1. **Campaign list** (`campaign_list_fetch`)
   - Paginate `campaigns.list` (100 per page)
   - Flatten report summary and recipient metadata
   - Write single `campaign_list.json`

2. **Campaign reports** (`campaign_report_fetch`)
   - Query BigQuery for campaign IDs sent in last 90 days
   - For each ID: `reports.get_campaign_report`
   - Up to 10 retries per campaign on failure
   - Write `campaign_reports_{campaign_id}.json` per campaign

3. **Click report** (`click_report_fetch`)
   - Same campaign ID list
   - Paginate `get_campaign_click_details` (1000 per page)
   - Write `click_report_{campaign_id}.json`

4. **Unsubscribes** (`unsubscribes_fetch`)
   - Paginate `get_unsubscribed_list_for_campaign`
   - Flatten merge fields (customer_id, segment, company_name, etc.)
   - Write `unsubscribes_{campaign_id}.json`

5. **Email activity** (`email_activity_fetch`)
   - Paginate `get_email_activity_for_campaign`
   - Nested `activity` array preserved for open/click events
   - Write `email_activity_{campaign_id}.json`

6. **Recipients** (`recipients_fetch`)
   - Paginate `get_campaign_recipients`
   - Flatten delivery status, open counts, merge fields
   - Write `recipients_{campaign_id}.json`

Chain: `start → campaign_list → campaign_reports → click_report →
unsubscribes → email_activity → recipients → pause`

## Stage B — Per-entity load (parallel fan-out)

For each entity in `name_list`, a independent chain runs from `pause`:

1. **GCSToGCS** — copy `data/mailchimp/{entity}/*.json` from Composer bucket
   to `mailchimp/{entity}/{yyyy-mm-dd}/` in raw zone
2. **GCSToBigQuery** — NEWLINE_DELIMITED_JSON →
   `trusted_staging.mailchimp_{entity}` with `WRITE_APPEND`, day partitioning
3. **BigQueryToBigQuery** — copy staging → `trusted.mailchimp_{entity}` with
   `WRITE_TRUNCATE` (full snapshot refresh)

All six entity chains converge at `end`.

## 90-day campaign window

Downstream fetches call `_query_results()`:

```sql
SELECT DISTINCT campaign_id
FROM `{project}.trusted_staging.mailchimp_campaign_list`
WHERE DATE(sent_time) >= CURRENT_DATE() - 90
```

Campaign list must complete and load before this query returns meaningful
results on first deploy. On steady-state daily runs, yesterday's campaign list
is already in staging from prior loads.

## Idempotency and re-runs

- Staging append is safe to re-run the same day (duplicate rows possible;
  downstream dbt dedup or trusted truncate handles analyst view).
- Trusted truncate gives a clean snapshot regardless of staging duplicates.
- Fetch tasks overwrite local JSONL files — re-running fetch replaces files
  before GCS copy.

## Failure modes worth knowing

- **Empty campaign list on first run**: downstream tasks query zero campaigns
  and complete quickly. Expected on cold start.
- **Rate limits**: 10-attempt retry per campaign absorbs most transient errors.
  Persistent failures log and move to the next campaign.
- **Large sends**: email activity and recipients can be high volume. Pagination
  at 1000 keeps memory flat; watch Composer worker disk for JSONL size.
- **PII in recipients/unsubscribes**: email addresses and merge fields are
  sensitive. Restrict BQ IAM to marketing/analytics roles; never log full rows.

## Example output

See `examples/` for synthetic JSONL samples per entity. These match the schema
files in `schemas/` and illustrate the flattened record shape after extraction.
