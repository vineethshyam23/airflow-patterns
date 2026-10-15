# Pattern 52: Offer Tool on-demand multi-project zone

Composer DAG with `schedule_interval=None` that publishes a *lean*
subset of warehouse refined / trusted tables into per-stage Offer Tool
GCP projects. Manual trigger for establishments, Elasticsearch search
projection, and catalog / geo / mapping refreshes.

Sibling of pattern 48 (weekday-aware scheduled zone with gaps /
assortment / scores). Same Wednesday stage fan-out policy; narrower
table set and dual country lists.

Source (read-only):
- `dags/etl_customized_offerings_zone_on_demand.py`
- `dags/horeca_digital/customized_offering_queries.py`

## Files

| File | Role |
|------|------|
| `dag_offer_tool_zone_on_demand.py` | Stage resolution, dual country loops, truncates |
| `zone_queries.py` | ES projection, ingredient images, soft-delete filter |
| `BUSINESS_CASE.md` | Why a second DAG instead of parameterizing #48 |
| `ARCHITECTURE.md` | Components + Mermaid diagram |
| `DATA_FLOW.md` | Paths A–C, failure modes |

## Quick start

```bash
python -c "import ast; ast.parse(open('zone_queries.py').read())"
python -c "import ast; ast.parse(open('dag_offer_tool_zone_on_demand.py').read())"
python -c "
import zone_queries as q
assert 'wholesale' in q.exclude_deleted_statement(field='wholesale_id', iso_code='DE')
assert 'benchmarking_gaps' in q.elasticsearch_data_query('prod', 'DE')
assert 'gold_ingredient' in q.ingredients_images_query()
print('queries ok')
"
# Full DAG import needs apache-airflow + google providers.
```

Needs BigQuery IAM on the DWH project (read) and each Offer Tool
project (write to `offer_tool_zone`), plus a GCP connection. This
folder is a sanitized reference, not a deploy package.

## Sanitization notes

- GCP projects `hd-dwh-stream-1*` → `dwh_project` / env `PROJECT`
- Offer Tool projects `customized-offering-*-#####` → `offer-tool-{dev,acc,stg,prod}`
- Dataset `cocs_bevelop` → `offer_tool_zone`
- `metro_*` / MCC naming → `wholesale_*`
- Product flags `has_Dish_*` / `has_MTO` → `has_pos` / `has_pay` / `has_order_tool`
- `cofg_relevant` → `offer_tool_relevant`
- `deepideas` menu source → `vendor_menu`
- Emails / owner → `dataops@example.com` / `data-platform`
- Package import `horeca_digital.customized_offering_queries` → local
  `zone_queries` (ES + images + soft-delete only)
- Unused `ShortCircuitOperator` import and dead `schedule_interval`
  variable from source removed; intentional `schedule_interval=None` kept
- `max_active_runs=1` and tags added

## Distinct from patterns 27 / 46 / 48

| | 27 | 46 | 48 | 52 (this) |
|---|----|----|----|-----------|
| Question | Land Offer Tool OLTP with history | Build refined Food Graph in DWH | Scheduled DWH → product projects | Manual lean DWH → product projects |
| Destination | DWH trusted SCD | `refined_foodgraph` | `offer-tool-*`.`offer_tool_zone` | same product dataset |
| Cadence | Daily sequential dumps | Daily country barrier | Weekday stage fan-out | On-demand (`None`) |
| Surface | 15 OLTP tables × SCD2 | Country analytics chains | Gaps / assortment / scores / recs | Establishments / ES / catalog |

## Category

`sql_patterns/52-customized-offerings-zone-on-demand/`
