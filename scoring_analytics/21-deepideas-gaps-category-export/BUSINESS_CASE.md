# Business case: Deepideas main-category gaps export

Assortment and account teams keep asking a blunt question: the
restaurant's menu points at a product main category, and we still
have zero wholesale revenue there for the last year. That is not the
same as "they buy less than peers" (pattern 16), not the same as a
ranked opportunity list for a recommender (12/14), and not the same
as "who is this establishment" (pattern 20). Collapsing those into
one partner schema made every consumer argue about filters and grain.

This pattern materializes the menu→ingredient→category need in
BigQuery staging, anti-joins last-year purchase revenue, and ships
only hash-delta rows to the partner event bus.

## What this unlocked

- One weekly Composer path that rebuilds "today", POSTs the delta,
  then advances the yesterday snapshot — same delta contract as
  establishment (20) and peer-gaps export helpers (16), without
  forcing ingredient-level gaps into the same reviewable sample
- A clear anti-join SLA: gap = menu-implied category with
  `revenue IS NULL` over one year. Easy to explain in a partner
  review; hard to accidental-change without noticing
- `_keyhash` / `_rowhash` so a quiet week costs almost nothing on
  the HTTP side. Production keyed `_keyhash` on customer id only;
  category grain lives in `_rowhash`. That is awkward but stable —
  changing it would rewrite soft-close semantics mid-flight
- Separation from establishment attributes so the Avro field count
  stays tiny (four fields) and partner consumers stay focused

## Constraints

- Active-buyer filter (last transaction within a year, status, branch
  groups) is the volume boundary. Exporting the full foodgraph
  establishment universe blows Composer slots and partner quotas.
- The join path is deep: establishments → menus → recipes /
  extracted ingredients → ingredients → product main category →
  prioritization relevance, then anti-join transactions. Failures
  are usually upstream freshness or mapping coverage, not Avro.
- `avg_relevance` travels as a string because the registered partner
  schema said so. Cast in the send SELECT; do not "fix" types in
  Python unless the schema registry changes.
- Production ran gaps_category after establishment in one sequential
  Deepideas DAG. Isolating the category feed here is deliberate —
  sibling patterns stay separately reviewable.

## What this is not

Not peer benchmarking gaps (16). Not ranked menu-gap batches
(12/14). Not establishment attribute enrichment (20). Not
ingredient-level Deepideas gaps (backlog). Not a real-time scoring
API. This stops at "weekly category-level zero-purchase gaps for
active buyers land on the partner bus when the row hash changes."
