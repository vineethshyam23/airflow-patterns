# Business case: Adyen payment terminal integration

Payment terminals sit in two worlds at once. Adyen owns the device inventory
and store assignment in their Management API. The ERP side owns which
serial should belong to which outlet. When those drift — terminal still in
inventory, wrong store, tip/standalone flags left on for a lite POS profile —
support ends up clicking through the Adyen portal one device at a time.

We needed a single nightly job that:

1. Refreshed the warehouse view of merchants / stores / terminals / settings
2. Let dbt compute the serial↔store match against ERP
3. Applied controlled write-backs (reassign + default settings PATCH) only for
   rows the match table flagged

## What this unlocked

- A trusted terminal inventory in BigQuery without manual CSV exports
- Reassignment that followed the ERP source of truth instead of tribal knowledge
- A repeatable way to force lite POS defaults (e.g. disable standalone tips)
  across a filtered set of devices

I cared more about operability than cleverness. The extract path is boring
JSONL → GCS → staging truncate. The interesting part is the management API
TaskGroup at the end: it only runs after dbt has refreshed the match model,
and the settings PATCH still fires (`ALL_DONE`) if reassign partially fails
so we do not strand a half-updated fleet waiting for a perfect reassign run.

## Constraints

Adyen rate limits and flaky pages are real. Pagination retries are short
(two retries, 60s base delay). Terminal settings GETs skip devices still in
`inventory` assignment — those endpoints are noisy and usually useless until
the device is assigned. Inter-request sleep on settings is tiny (50ms); enough
to avoid stampedes, not a full backoff policy.

Write-backs fail the task if any row fails after retries. Partial success is
logged as a CSV summary in the task log so on-call can see which terminal_ids
need a second look. I preferred a loud failure over silently leaving drift.

## What this is not

Not a full payments ledger. Not reconciliation of settlement amounts. It is
device inventory + controlled configuration sync between Adyen and the
warehouse/ERP match layer.
