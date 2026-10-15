# Business case: Deepideas gap-ingredients export

Assortment and offer teams keep asking a sharper question than "which
main category is missing?": the restaurant's menu already uses an
ingredient, and we still have zero wholesale revenue on articles mapped
to that ingredient for the last year. That is not the same as
category-level zero purchase (pattern 21), not peer under-index (16),
not a ranked opportunity list (12/14), and not "who is this
establishment" (20). Collapsing ingredient and category into one
partner schema made every consumer argue about grain.

This pattern materializes the menu→recipe→ingredient need in BigQuery
staging, anti-joins last-year purchase revenue at ingredient grain, and
ships only hash-delta rows to the partner event bus.

## What this unlocked

- One weekly Composer path that rebuilds "today", POSTs the delta,
  then advances the yesterday snapshot — same delta contract as
  establishment (20) and category gaps (21), without forcing category
  aggregation into the ingredient Avro contract
- A clear anti-join SLA: gap = menu-implied ingredient with
  `revenue IS NULL` over one year on `wholesale_id × ing_id`. Easy to
  explain in a partner review; hard to accidental-change without
  noticing
- `_keyhash` / `_rowhash` so a quiet week costs almost nothing on the
  HTTP side. Production keyed `_keyhash` on customer id only;
  ingredient grain lives in `_rowhash`. Awkward but stable — changing
  it would rewrite soft-close semantics mid-flight
- Separation from category gaps so partner consumers stay focused and
  the field list stays tiny (four fields)

## Constraints

- Active-buyer filter (last transaction within a year, status, branch
  groups) is the volume boundary. Ingredient grain is denser than
  category; widen the filter without checking partner quotas and the
  weekly POST becomes a multi-hour job.
- Join path is thinner than category gaps (no extracted-ingredient
  union), but still depends on recipe and article→ingredient mapping
  coverage. Failures are usually upstream freshness, not Avro.
- `relevance` travels as a string because the registered partner
  schema said so. Cast in the send SELECT; do not "fix" types in
  Python unless the schema registry changes.
- Production ran gap_ingredients after gaps_category in one sequential
  Deepideas DAG. Isolating the ingredient feed here is deliberate —
  sibling patterns stay separately reviewable.

## What this is not

Not main-category zero-purchase gaps (21). Not peer benchmarking gaps
(16). Not ranked menu-gap batches (12/14). Not establishment attribute
enrichment (20). Not a real-time scoring API. This stops at "weekly
ingredient-level zero-purchase gaps for active buyers land on the
partner bus when the row hash changes."
