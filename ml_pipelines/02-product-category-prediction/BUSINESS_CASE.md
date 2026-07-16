# Business case: POS product category prediction

POS catalogs are messy. Cashiers name items however they like — "Cola 0.3",
"Hauscola", "Softdrink Cola" — and family groups are not a reliable taxonomy
across outlets. Analytics and menu engineering still need a stable
food vs beverage (and finer) label.

Manual tagging does not scale when thousands of new SKUs show up every week
across a multi-country POS estate. We trained a lightweight text classifier
on historical labeled products and ran it as a daily batch job over names
that had never been scored.

## What this unlocked

- Self-service product views with a consistent category flag without waiting
  on ops to classify every new item
- Downstream menu / assortment analysis that could filter by predicted class
  instead of free-text name patterns
- Incremental cost: we only scored names missing from the prediction table,
  so re-runs were cheap once the backlog was cleared

## Constraints that shaped the design

Composer workers already had sklearn/joblib. Pulling a pickle from GCS and
scoring in Python was simpler than standing up a Vertex endpoint for a
model that only needed to run once a day on a few thousand strings.

I kept retries light (one retry, 10 minutes). Failure mode is "yesterday's
predictions are still valid; new names wait until tomorrow." That matched
the SLA — analysts did not need intra-day refreshes for category labels.

## What this is not

Not online inference. Not a substitute for a curated product master when
you have one. It is a pragmatic layer on top of noisy POS naming so reporting
does not stall.
