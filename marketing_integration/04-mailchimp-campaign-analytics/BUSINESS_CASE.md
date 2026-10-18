# Business case: Mailchimp campaign analytics integration

Marketing ran dozens of email campaigns per quarter across multiple audience
lists. Performance lived in the Mailchimp UI: open rates, click maps,
unsubscribe reasons, per-recipient activity. Analytics and CRM teams needed
that data in the warehouse next to customer, subscription, and sales data.

The Mailchimp UI is fine for campaign managers. It is not fine when you want
to answer questions like "which segment unsubscribes most after product
updates?" or "do customers with open support tickets engage less with
promotional sends?" Those joins need BigQuery.

## What this pipeline delivers

Six entities, refreshed nightly:

1. **Campaign list** — metadata and summary metrics for every campaign
2. **Campaign reports** — detailed delivery and engagement stats per campaign
3. **Click report** — URL-level click counts and percentages
4. **Unsubscribes** — who opted out, when, and why (with CRM merge fields)
5. **Email activity** — per-recipient open/click event streams
6. **Recipients** — delivery status and open counts per sent address

Downstream dbt models and Looker dashboards consume the trusted tables. The
pipeline has run daily since late 2022 with minimal manual intervention.

## Design constraints

- **90-day lookback**: downstream entity fetches only query campaigns sent in
  the last 90 days from staging. Keeps API volume bounded as the campaign
  archive grows.
- **Sequential fetch, parallel load**: all six API extractions run in series
  (campaign list must land before the BQ lookup works), then six independent
  GCS → BQ → trusted copy chains fan out from a pause node.
- **Per-campaign retry**: report endpoints retry up to 10 times per campaign
  ID. Mailchimp rate limits and transient 5xx errors are common on large sends.
- **Append staging, truncate trusted**: staging accumulates daily partitions;
  trusted is full replace each run for analyst-friendly "current snapshot" queries.

## What I would do differently today

- Run campaign-list-independent entities in a TaskGroup with dynamic task
  mapping instead of one long sequential chain.
- Push schemas to the Composer bucket via CI instead of manual upload.
- Add row-count freshness checks against Mailchimp `total_items` in the API
  response before marking the DAG green.
