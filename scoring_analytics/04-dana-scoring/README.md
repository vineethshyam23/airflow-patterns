# Pattern 04: Multi-country FBO/NBO scoring export

Monthly pipeline that unions First-Best-Offer and Next-Best-Offer scores
across markets, detects hash-based deltas against last month's history, and
pushes only changed rows as Avro events to an external ingest API.

Source (read-only):
- `dags/horeca_digital/dana_scoring_query.py`
- `dags/horeca_digital/dana_scoring_export.py`
- `dags/etl_dana_scoring_data_export.py`

## Files

| File | Role |
|------|------|
| `scoring_query.py` | Multi-country SQL builders + keyhash/rowhash delta helpers |
| `scoring_export.py` | OAuth client, Avro encode, chunked bulk POST |
| `dag_scoring_data_export.py` | Composer DAG: today truncate → ingest × N → hist append/expire |
| `BUSINESS_CASE.md` | Why monthly hash-delta beats full reload |
| `ARCHITECTURE.md` | Components + Mermaid diagram |
| `DATA_FLOW.md` | Run order, idempotency, failure modes |

## Quick start

```bash
python -c "import ast; ast.parse(open('scoring_query.py').read())"
python -c "import ast; ast.parse(open('scoring_export.py').read())"
python -c "import ast; ast.parse(open('dag_scoring_data_export.py').read())"
python scoring_query.py   # prints the DE country SELECT
```

To run for real you need BigQuery refined/trusted tables, Airflow Variables
for the event API OAuth + schema id, and the staging today/hist tables. This
folder is a sanitized reference, not a deploy.

## Sanitization notes

- GCP projects `hd-dwh-stream-*` → `dwh_project` / `dwh_project_dev`
- Datasets `dwh_refined` / `dwh_trusted_staging` / `dwh_trusted_mcc` →
  `refined` / `trusted_staging` / `trusted_mcc`
- Event API host / schema ids / OAuth Variable names generalized
- Real notification emails → `dataops@example.com`
- Owner / author names removed
- Package import `horeca_digital.*` → local `scoring_query` / `scoring_export`
- Inline credential literals were already Variable-backed; still, never
  commit real client secrets
- Minor SQL cleanups: string literals in CONCAT, aligned `score_type` column
  name on the NBO branch for UNION safety

## Category

`scoring_analytics/04-dana-scoring/`
