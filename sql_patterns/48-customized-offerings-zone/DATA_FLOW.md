# Data Flow: Offer Tool zone publish

## Schedule

Production typically runs on-demand / trigger-aligned after refined Food
Graph and scoring tables are fresh (`schedule_interval=None` in this
portfolio DAG). DEV applies an additional Monday ShortCircuit on the
benchmarking branch.

`max_active_runs=1` — a second trigger while Wednesday multi-stage is
running would double slot spend for no freshness gain.

## Path A — stage resolution

```
env PROJECT
  ├─ *_dev        → [(dev, offer-tool-dev)]
  └─ prod
       ├─ Wed     → [(acc, …), (stg, …), (prod, …)]
       └─ else    → [(prod, offer-tool-prod)]
```

Every downstream truncate loops this list. Changing weekday policy is
one function, not N task edits.

## Path B — Food Graph gaps + validation

For each active stage:

1. Nest unnested trusted gaps → `{stage_project}.offer_tool_zone.fg_gaps`
2. Edge into shared `fg_gaps_validation` (writes DWH `monitoring`)
3. `fg_gaps_alert` reads monitoring rows and notifies

Validation always compares against **prod** nested + trusted `_acc`
unnested — the product contract that matters for sales.

## Path C — country snapshots (no barrier)

Independent WRITE_TRUNCATE families (no DummyOperator barrier — product
tolerates partial country freshness better than blocking all markets):

| Family | Countries | Notes |
|--------|-----------|-------|
| wholesale_assortment | 11 (AT uses DE assortment source) | cluster `art_no` |
| wholesale_masterdata | 11 | exclude deleted unique + card id |
| analytics_visit | 11 | DAY on `date`, cluster wholesale_id |
| analytics_article | 11 | DAY on `last_purchase_date` |
| fbo_nbo_scores | 13 | DE/FR richer POS potential join |
| article_recommender | PL, DE, PT | DAY on `creation_date` |

## Path D — benchmarking (Monday-gated in DEV)

`monday_only_for_dev` >> gaps / topsellers / skeletons.

Skeletons are wildcard `benchmarking_gaps_skeletons_*` into a single
table per stage. Country gaps keep JSON array trimmed with
`LTRIM/RTRIM(TO_JSON_STRING(...))` for the product parser.

## Path E — article recommendation

1. Per stage: build `article_recommendation_branch` (top-80 revenue
   customers, own-brand / frequency / FAISS blend).
2. Per country in {DE, PL, FR, HR, NL, PT, ES}: join country purchases
   to that stage's branch table.

Edge: `article_recommendation_branch_{stage} >> article_recommendation_{iso}`.

## Failure and recovery

| Symptom | Likely cause | Action |
|---------|--------------|--------|
| Acc/stg stale mid-week | Expected — only Wed widens stages | Wait for Wednesday or manual trigger with forced stage list |
| DEV silent Tue–Sun | ShortCircuit | Expected; force-run clears gate only if you bypass task |
| Empty gap_ingredients in UI | Nest/unnest drift | Check monitoring table + alert logs |
| Country missing in one stage | That stage's BQ job failed | Clear that task; others need not rerun |
| Slot timeout Wed | Triple fan-out | Reservation or stagger after pattern 46 |

## Downstream consumers

- Offer Tool backend (product GCP projects) — primary reader.
- Sibling on-demand DAG (`etl_customized_offerings_zone_on_demand`) —
  establishments / ingredients / elasticsearch slices; not shipped in
  this pattern folder.
- Pattern 27 SCD ingest — separate OLTP path; shares naming only.
