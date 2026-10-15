# Pattern 26: Single-market Order + Reservation monthly export

Monthly full-load of one market's platform Order and Reservation
lifetime rows to the partner event bus. One dbt Cloud job refreshes
both refined tables; Composer then ships two Avro contracts through a
shared chunked ingest helper.

Distinct from pattern 23 (multi-country product footprint with
staging truncate) and pattern 24 (MAG acquisition / penetration
aggregates). This feed answers "what Order and Reservation activity
exists for this market's establishments, with latest subscription
attrs?" — not footprints, not management rates.

Source (read-only):
- `dags/etl_dana_pl_dish_orders_reservations_export.py`
- `dags/horeca_digital/dana_pl_dish_orders_export.py`
- `dags/horeca_digital/dana_pl_dish_orders_query.py`

## Files

| File | Role |
|------|------|
| `orders_query.py` | Typed SELECTs for orders + reservations refined tables |
| `orders_export.py` | Shared Avro + chunked POST sender for both contracts |
| `dag_orders_reservations_export.py` | Monthly Composer wiring (dbt → dual export → end) |
| `BUSINESS_CASE.md` | Why one DAG / one dbt job / full monthly reship |
| `ARCHITECTURE.md` | Components + Mermaid diagram |
| `DATA_FLOW.md` | Schedule, grain, failure modes |

## Quick start

```bash
python -c "import ast; ast.parse(open('orders_query.py').read())"
python -c "import ast; ast.parse(open('orders_export.py').read())"
python -c "import ast; ast.parse(open('dag_orders_reservations_export.py').read())"
python orders_export.py
```

Needs `refined.market_dish_orders` / `refined.market_dish_reservations`,
event-API OAuth Variables, two registered schema ids, and
`dbt_market_dish_orders_job_id`. This folder is a sanitized reference,
not a deploy.

## Sanitization notes

- GCP projects `hd-dwh-stream-*` → `dwh_project` / `dwh_project_dev`
- Dataset `dwh_refined` → `refined`
- Tables `dana_pl_dish_orders` / `dana_pl_dish_reservations` →
  `market_dish_orders` / `market_dish_reservations`
- Columns `odoo_metro_id` / `odoo_store_id` → `wholesale_id` / `store_id`
- Event API host / schema ids / OAuth Variable names generalized
- dbt job id → Airflow Variable `dbt_market_dish_orders_job_id`
- Real notification emails → `dataops@example.com`
- Owner / author names and internal ticket ids removed
- Package imports `horeca_digital.*` → local modules
- Local `batched` helper replaces internal utils import
- `dbt_poll_interval` amortized helper → fixed `timeout // 10` check
- Avro schema parsed once per send; 401 retry keeps payload

## Distinct from patterns 23 / 24

| | 23 | 24 | 26 (this) |
|---|----|----|-----------|
| Question | Product footprint of matched customers | Acquisition $ + penetration rates | Order + Reservation lifetime activity |
| Cadence | Daily staging → dbt → ingest | Monthly full history reship | Monthly dbt → dual full-load |
| Grain | wholesale_id × country footprint | date × bundle / rates | order_id / reservation_id + asset envelope |
| Markets | 14 parallel | 17 sequential × 2 chains | 1 market, 2 product contracts |

## Category

`scoring_analytics/26-pl-dish-orders-reservations/`
