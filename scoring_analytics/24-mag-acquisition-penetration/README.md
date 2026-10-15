# Pattern 24: MAG acquisition + penetration monthly export

Monthly ship of wholesale acquisition dollars (by product bundle) and
wholesale→platform penetration rates to the partner event bus. One
Composer DAG owns two independent sequential country chains — same
schedule, different Avro contracts.

Distinct from pattern 04 (FBO/NBO scores), pattern 17 (establishment
market listings), and pattern 23 (per-customer product footprint).
This feed answers "what did we acquire, and how deep is platform
penetration?" at market-month grain — not scores, not listings, not
customer footprints.

Source (read-only):
- `dags/etl_dana_mag_export.py`
- `dags/horeca_digital/dana_mag_acquisition.py`
- `dags/horeca_digital/dana_mag_penetration.py`

## Files

| File | Role |
|------|------|
| `mag_acquisition.py` | Acquisition SELECT → Avro → chunked POST |
| `mag_penetration.py` | Penetration SELECT → Avro → chunked POST |
| `dag_mag_export.py` | Monthly Composer wiring (two ALL_DONE chains) |
| `BUSINESS_CASE.md` | Why two chains, why full monthly reship |
| `ARCHITECTURE.md` | Components + Mermaid diagram |
| `DATA_FLOW.md` | Schedule, country mapping, failure modes |

## Quick start

```bash
python -c "import ast; ast.parse(open('mag_acquisition.py').read())"
python -c "import ast; ast.parse(open('mag_penetration.py').read())"
python -c "import ast; ast.parse(open('dag_mag_export.py').read())"
python mag_acquisition.py
python mag_penetration.py
```

Needs `refined.hist_acquisitions_reporting` /
`refined.hist_penetration_rates_reporting`, event-API OAuth Variables,
and two registered schema ids. This folder is a sanitized reference,
not a deploy.

## Sanitization notes

- GCP projects `hd-dwh-stream-*` → `dwh_project` / `dwh_project_dev`
- Dataset `dwh_refined` → `refined`
- Tables `hist_acquisitions_mag_reporting` /
  `hist_penetration_rates_mag_reporting` →
  `hist_acquisitions_reporting` / `hist_penetration_rates_reporting`
- Avro fields `active_MCC` / `buying_MCC` / `active_HD` / `paying_HD`
  → `active_wholesale` / `buying_wholesale` / `active_platform` /
  `paying_platform`
- Special market `mi` (warehouse `hd`) → `ag` (warehouse `corp`)
- Event API host / schema ids / OAuth Variable names generalized
- Real notification emails → `dataops@example.com`
- Owner / author names removed
- Package imports `horeca_digital.*` → local modules
- Avro schema parsed once per send; 401 retry keeps payload
- `max_active_runs` set on the DAG constructor

## Distinct from patterns 04 / 17 / 23

| | 04 | 17 | 23 | 24 (this) |
|---|----|----|----|-----------|
| Question | FBO/NBO scores | Market listing attrs | Product footprint of matched customers | Acquisition $ + penetration rates |
| Cadence | Export batches | Monthly full load | Daily staging → dbt → ingest | Monthly full history reship |
| Grain | Score rows | Listing per country | wholesale_id × country footprint | date × bundle / date × rates |
| Markets | Multi | Multi sequential | 14 parallel | 17 sequential × 2 chains |

## Category

`scoring_analytics/24-mag-acquisition-penetration/`
