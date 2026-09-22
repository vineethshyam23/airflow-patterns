# Data Flow: Offer Tool on-demand zone

## Schedule

`schedule_interval=None` — triggered from the UI, CLI, or an upstream
sensor when product asks for a lean refresh. No Monday SoftCircuit.

`max_active_runs=1` — a second manual trigger while a Wednesday
multi-stage run is in flight would double slot spend for the same
destination tables.

## Path A — establishments (16 countries)

```
for iso in ESTABLISHMENT_COUNTRIES:
  for (stage, project) in resolve_stages(...):
    WRITE_TRUNCATE project.offer_tool_zone.all_establishments_{iso}
      ← refined.all_establishments_{iso} (offer_tool_relevant, data_source=all)
        ⟕ platform_customer_base_establishment (product flags)
        ⟕ all_wholesale_establishments_{iso} (display name)
```

Independent jobs. No barrier — a failed `RS` task does not block `DE`.

## Path B — stage-level catalog / geo / menus

For each active stage (1 on most days, 3 on Wednesday):

| Task family | Source | Destination table |
|-------------|--------|-------------------|
| fg_ingredients / translations | `trusted.fg_*_{dev\|acc}` | `fg_ingredients_{stage}` (+ translations) |
| gold ingredient images | trusted ingredients ⋈ translations | `fg_gold_ingredients_images` |
| articles → ingredients | trusted article map ⋈ translations | `fg_articles_to_ingredients_mapping` |
| ingredient (+ recommendation) | `refined.offer_tool_ingredient*` | `ingredient` / `ingredient_recommendation` |
| zip geo | `refined.zip_region_wholesale` | `zip_code_region_coordinates` |
| wholesale stores | `refined_foodgraph.vwholesale_stores` | `wholesale_stores` |
| all_mappings | `refined.all_mappings` + soft-delete filter | `all_mappings` |
| menus | `all_menu_items` / website builder / vendor | `*_menu*` tables |
| data tool recommendation | `refined_foodgraph.vdata_tool_recommendation` | `data_tool_recommendation` |

## Path C — Elasticsearch projection (11 countries)

```
for iso in ELASTICSEARCH_COUNTRIES:
  for (stage, project) in resolve_stages(...):
    WRITE_TRUNCATE project.offer_tool_zone.elasticsearch_data_{iso}
      ← elasticsearch_data_query(stage, iso)
```

Projection joins establishments to benchmarking potential categories and
wholesale display names, then emits the flattened search document shape
(product keeps null placeholders for region / category arrays filled
elsewhere).

## Stage resolution reminder

```
env PROJECT
  ├─ *_dev        → [(dev, offer-tool-dev)]
  └─ prod
       ├─ Wed     → [(acc, …), (stg, …), (prod, …)]
       └─ else    → [(prod, offer-tool-prod)]
```

Resolved once at DAG parse. Re-triggering the same parsed DAG on Thursday
still uses Thursday's stage list only if Composer has reparsed after the
weekday change — in practice prod Composer reparses on code sync / daily
refresh, so Wednesday widening is reliable for ops.

## Failure and recovery

| Symptom | Likely cause | Action |
|---------|--------------|--------|
| Acc/stg still stale after manual run | Non-Wednesday stage list | Wait for Wed or force stage list in a feature branch |
| Empty ES table for CZ | CZ not in ELASTICSEARCH_COUNTRIES | Expected — establishments-only market |
| Deleted cards in UI | Soft-delete filter miss on mappings | Re-run `*_all_mappings`; check field names |
| Overlap with #48 Wednesday spike | Both DAGs fan out | Prefer one; clear the lighter on-demand run |

## Downstream consumers

- Offer Tool backend search + establishment card readers in product GCP
  projects.
- Pattern 48 remains the source of truth for gaps / assortment / scores —
  this DAG does not replace it.
