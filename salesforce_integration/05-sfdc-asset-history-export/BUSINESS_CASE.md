# Business case: Salesforce asset history delta export

CRM holds the authoritative view of which products are installed at which
establishments — channel, referrer, install/disable dates, shipping address,
account identifiers. Downstream analytics and partner systems need those
changes as events, not a nightly dump of every active asset.

This pattern is the bridge: materialize a daily SFDC asset snapshot in
BigQuery, detect what changed since yesterday with keyhash/rowhash, and push
only the delta as Avro events to an external ingest API. An SCD-style history
table keeps the audit trail.

## What this unlocked

- Daily CRM → event-bus without replaying the full asset universe
- Hash-based change detection so quiet nights cost almost nothing on the API
- A history table that keeps superseded install/status versions for "what
  left the platform on date X" questions without rebuilding from CRM

I cared about the ordering constraint as much as the SQL. History has to stay
frozen while ingest runs. Append/expire first and the delta query returns
zero — looks like a green DAG with a silent bus.

## Constraints

- Schedule is daily (`40 3 * * *`), after the overnight CRM → warehouse sync.
  Monthly would miss install/disable churn that sales ops watches same-week.
- Pilot started on one market (ES). The country list and ISO map are left as
  structures so a second market can be added without reshaping the DAG.
- Avro bulk posts in chunks of 500. Asset volume is smaller than scoring, so
  chunk size is more about matching the shared event-API contract than
  throughput.
- Key identity is establishment UID + CRM account identifier. Product and
  status live in the rowhash — a status flip on the same establishment
  produces a new event without inventing a separate CDC stream.

## What this is not

Not Salesforce Bulk API extraction (that lives upstream of the refined
table). Not CRM write-back. It is warehouse → event bus for asset history,
with an SCD-lite side effect so we can prove what changed each day.
