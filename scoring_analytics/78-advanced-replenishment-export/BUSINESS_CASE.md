# Business case: Advanced Replenishment partner export

Wholesale replenishment planners needed predicted *ingredient demand*
per establishment — not another menu-gap ranking and not a POS
article recommendation list. Upstream Vertex / foodgraph already
lands
`advanced_replenishment_predicted_total_quantity_{CC}` plus an
articles→ingredients bridge. This DAG is the export path onto the
partner event bus, with the customer dimension the consumer joins
on.

I put three subjects in one DAG instead of three. The quantity feed
is daily and large enough to need hash partitions. The customer and
articles feeds are slower-changing reference tables — shipping them
every day burns ingest quota for little partner value. A
`BranchPythonOperator` on UTC day == 5 keeps the monthly arm in the
same DAG so ops sees one schedule and one failure surface.

## What this unlocked

- Daily predicted-quantity Avro feed without loading the full DE
  table into a single worker (10 × `FARM_FINGERPRINT` batches)
- Monthly POS-customer + articles-to-ingredients snapshots on the
  same run that already authenticated to the event API
- Auth that prefers `client_credentials`, retries throttled IdP
  responses before password-grant fallback, and staggers parallel
  token requests so ten batches do not 403 the identity provider
- Explicit type mapping (BQ INT64 → Avro long, FLOAT64 → double,
  DATE → string) so the registered schemas stay honest

## Constraints

- Schedule is `15 6 * * *`. The Vertex / foodgraph job that lands
  the predicted-quantity table must finish earlier that morning.
  Prefer a Dataset sensor over hoping cron order holds.
- `max_active_tasks=10` matches `TOTAL_BATCHES`. Raising batches
  without raising the pool just serializes work; raising the pool
  without staggering tokens will trip IdP rate limits.
- Full load, no date filter. The partner treats each subject as a
  snapshot. Re-runs re-post the same rows — coordinate before a
  historical replay.
- `end` uses `ALL_DONE`. A failed batch does not freeze the monthly
  arm (or vice versa) — good for partial delivery, bad if "DAG
  green" is treated as full coverage.
- Customer query still joins behavioural segmentation from the
  discovery project. That is intentional until prod segmentation
  is cut over — document the flip in the runbook when it happens.

## What this is not

Not ranked menu-gap opportunities (patterns 12/14). Not POS
Intelligence *article* recommendations (pattern 53). Not FBO/NBO
scoring (pattern 04). Not the Vertex pipeline that *builds* the
prediction table — only the export and the join tables the partner
needs beside it.
