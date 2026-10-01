# Pattern 61: Derived events change-detection (SCD LAG → append)

Daily Composer DAG that turns historized CMS / Adobe / Reservation Tool
tables into an append-only business-event store. Production is a ~58-task
sequential monolith (~3.2k lines of inline SQL). This folder ships a
**representative subset** covering every detection style in that chain.

Distinct from pattern 01 (Matching Engine SCD2 table build), pattern 47
(activity scores that *consume* derived events), and pattern 54
(Reservation Tool Cloud SQL land). Here the warehouse SCD already
exists — the job is change detection into a shared event contract.

Source (read-only):
- `dags/etl_derived_events.py`

## Files

| File | Role |
|------|------|
| `dag_derived_events.py` | Six sequential ReservedBigQuery insert jobs |
| `event_queries.py` | SQL builders for the six detection styles |
| `bq_reservation.py` | Night-ETL BigQuery reservation wrapper |
| `BUSINESS_CASE.md` | Why a central derived_events store |
| `ARCHITECTURE.md` | Components + Mermaid diagram |
| `DATA_FLOW.md` | Schema, hash rules, re-run behaviour |

## Quick start

```bash
python -c "import ast; ast.parse(open('bq_reservation.py').read())"
python -c "import ast; ast.parse(open('event_queries.py').read())"
python -c "import ast; ast.parse(open('dag_derived_events.py').read())"
python -c "import event_queries as q; print(q.query_cms_modification_date()[:120])"
python -c "import event_queries as q; print(len(q.query_adobe_datafeed('trusted.derived_events')))"
```

To run for real you need historized `trusted.cms_*`, `trusted.adobe_*`,
`trusted.reservation_*` tables, Variables `derived_events_bq_project` /
`derived_events_bq_conn`, and a BigQuery connection with insert rights
on `trusted.derived_events`. This folder is a sanitized reference, not
a deploy of the full 58-task DAG.

## Sanitization notes

- GCP project `hd-dwh-stream-1` → Variable `derived_events_bq_project`
  (default `dwh_project`)
- Datasets `dwh_trusted` / `dwh_trusted_views` → `trusted` /
  `trusted_views`
- Product CMS `hyd_*` / Hydra → `cms_*`; ResTool `rt_*` tables →
  `reservation_*` (event *names* kept as `RT …` to match the store
  contract consumers already use)
- Adobe `aa_*` → `adobe_*`; Custom Event labels brand-scrubbed
  (`dish_*` → `platform_*`, `mk_*` → `recipe_tool_*`, etc.) — mapping
  truncated to a representative set, not all 60+ production WHEN arms
- Owner / emails → `data-platform` / `dataops@example.com`
- Package import `horeca_digital.bq_reservation` → local module
- Inline SQL extracted into `event_queries.py`
- Added `max_active_runs=1`, tags, description
- Subset only: 6 of ~58 tasks (2 CMS + Adobe + 3 Reservation)

## Distinct from nearby patterns

| | 01 SCD2 | 47 Activity scores | 54 Reservation land | 61 (this) |
|---|---|---|---|---|
| Input | Matching engine facts | Refined channel bases | Cloud SQL OLTP | Already-historized trusted |
| Output | SCD2 dimension | Score + MoM tables | Trusted RT tables | Append-only event store |
| Core trick | Merge historization | Threshold fan-in | Id watermark / Sunday full | LAG / unnest + `_rowhash` |

## Category

`sql_patterns/61-derived-events-change-detection/`
