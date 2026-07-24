# Pattern 10: Matching engine export to partner event bus

Weekly pipeline that rebuilds a multi-country matching staging table
(current valid matches × product footprint), then fans out per-country
Avro bulk ingest to a partner event API.

Distinct from pattern 01 (SCD Type 2 history of `match_result`). This
pattern ships the *current* valid matches shaped as service rows.

Source (read-only):
- `dags/horeca_digital/matching_export_to_DANA.py`
- `dags/horeca_digital/dana_matching_engine_export.py`
- `dags/horeca_digital/archived/etl_dana_matching_engine_export.py`

## Files

| File | Role |
|------|------|
| `matching_prepare.py` | Per-country SQL + pandas unpivot → staging replace |
| `matching_event_export.py` | OAuth client, Avro encode, chunked bulk POST |
| `dag_matching_engine_event_export.py` | Composer DAG: prepare → parallel country ingest |
| `BUSINESS_CASE.md` | Why prepare + ingest share one DAG |
| `ARCHITECTURE.md` | Components + Mermaid diagram |
| `DATA_FLOW.md` | Run order, idempotency, failure modes |

## Quick start

```bash
python -c "import ast; ast.parse(open('matching_prepare.py').read())"
python -c "import ast; ast.parse(open('matching_event_export.py').read())"
python -c "import ast; ast.parse(open('dag_matching_engine_event_export.py').read())"
python matching_prepare.py          # prints a sample country SQL prefix
python matching_event_export.py     # Avro schema parse smoke check
```

To run for real you need the match_result / customer base / MCC tables,
event-API OAuth Variables, and a registered schema id. This folder is
a sanitized reference, not a deploy.

## Sanitization notes

- GCP projects `hd-dwh-stream-*` → `dwh_project` / `dwh_project_dev`
- Datasets `dwh_refined` / `dwh_trusted` / `dwh_trusted_mcc` /
  `dwh_discovery` → `refined` / `trusted` / `trusted_mcc` / `staging`
- Table `mcc_hd_matching_export_to_DANA` → `partner_matching_export`
- Event API host / schema ids / OAuth Variable names generalized
- Real notification emails → `dataops@example.com`
- Owner / author names removed
- Package imports `horeca_digital.*` → local modules
- `DummyOperator` → `EmptyOperator` (with fallback)
- Avro schema parse moved outside the per-row loop
- HTTP errors now raise (`raise_for_status`)
- 401 retry now re-sends the original payload
- DATE / decimal Avro logical types encoded explicitly

## Category

`sql_patterns/10-matching-engine-event-export/`
