# Marketing integration patterns

Airflow DAG patterns for marketing platform APIs: email campaign analytics,
audience engagement, and click tracking.

## Patterns

| # | Pattern | Folder |
|---|---------|--------|
| 04 | Mailchimp campaign analytics | [`04-mailchimp-campaign-analytics/`](04-mailchimp-campaign-analytics/) |

Daily extract of six Mailchimp report entities into BigQuery: campaign list,
reports, click details, unsubscribes, email activity, and recipients.

## Common themes

- Paginated API extraction with retry loops
- JSONL landing on Composer → GCS raw zone → BigQuery staging → trusted snapshot
- Audience merge fields flattened for CRM joins
- 90-day lookback window to bound API volume

## Related

- Portfolio showcase: [Mailchimp Marketing Analytics](https://github.com/vineethshyam23/data-platform-portfolio/tree/main/showcase/09-mailchimp-marketing-analytics)
