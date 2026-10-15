# Business case: Mailchimp email analytics ingest

A regional ESP instance held campaign performance that marketing and
sales ops needed next to CRM and product usage — not just in the
Mailchimp UI. Opens, clicks, unsubscribes, and recipient-level
activity had to land in BigQuery on a daily cadence so suppression
lists and campaign scorecards stayed honest.

## Problem

Mailchimp's Marketing API is campaign-scoped for most report
endpoints. You cannot pull a single wide "yesterday's engagement"
extract; you list campaigns, then walk each campaign for reports,
click URLs, unsubscribes, activity, and recipients. At a few hundred
campaigns in a 90-day window that is a lot of sequential HTTP, and
activity/recipients endpoints time out often enough that a one-shot
fetch is unreliable.

## Approach

Composer owns the graph in two phases. Phase 1 runs six Python
extracts serially and writes JSONL under the Composer data volume.
Campaign list comes first. Report extractors then query
`trusted_staging.mailchimp_campaign_list` for IDs sent in the last
90 days and retry each campaign up to ten times. Phase 2 fans out
from a pause barrier: copy JSON into rawzone, APPEND into day-
partitioned staging, then WRITE_TRUNCATE trusted from staging so
analysts have a single current table per grain.

I kept the serial extract order and the APPEND→TRUNCATE land pattern
because that is how this ran in production. The tradeoff is staging
growth (APPEND every day) and a same-run dependency quirk: report
tasks read campaign IDs from staging that usually come from prior
days in the 90-day window, not from the campaign_list file just
written to disk in the same run.

## Why not Maileon-style parallel report branches?

Pattern 31 (Maileon) pages eight report types independently with an
empty-file branch and dbt downstream. Mailchimp's API shape is
different: five of six grains are keyed by campaign_id, so a shared
campaign inventory plus per-campaign retries matter more than
parallel report branches. No dbt step here — trusted is a straight
BigQuery table copy.

## Constraints

- API key and server prefix via Airflow Variables; nothing in code
- Recipients / unsubscribes / activity carry email addresses — PII
- Merge-field tags are account-specific; sanitized sample uses
  generic keys (map to your list's merge tags in a real deploy)
- Schema JSON objects under `schema_json/{entity}.json` must exist
  in the rawzone bucket for GCSToBigQuery
- Parse-time `loaddate` is an inherited footgun — prefer `{{ ds }}`

## Out of scope here

Sending campaigns, audience sync into Mailchimp, or Cloud Function
wrappers around the Marketing API belong elsewhere. This pattern is
warehouse ingest only.
