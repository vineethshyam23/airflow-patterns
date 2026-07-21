# Pattern 07: Odoo list-price / commission monthly delta export

Monthly pipeline that snapshots Odoo WSL invoice lines joined to an internal
list-price book, detects hash-based deltas against last month's history, and
pushes only changed commission rows as Avro events to an external ingest API.

Source (read-only):
- `dags/horeca_digital/dana_odoo_list_price_query.py`
- `dags/horeca_digital/dana_odoo_list_price_export.py`
- `dags/horeca_digital/archived/etl_dana_Odoo_list_price_export.py`

## Files

| File | Role |
|------|------|
| `list_price_query.py` | SQL builders + keyhash/rowhash delta helpers |
| `list_price_export.py` | OAuth client, Avro encode, chunked bulk POST |
| `dag_list_price_export.py` | Composer DAG: today truncate → ingest → hist append/expire |
| `BUSINESS_CASE.md` | Why monthly hash-delta beats full finance reload |
| `ARCHITECTURE.md` | Components + Mermaid diagram |
| `DATA_FLOW.md` | Run order, idempotency, failure modes |

## Quick start

```bash
python -c "import ast; ast.parse(open('list_price_query.py').read())"
python -c "import ast; ast.parse(open('list_price_export.py').read())"
python -c "import ast; ast.parse(open('dag_list_price_export.py').read())"
python list_price_query.py   # prints insert/send query previews
```

To run for real you need refined WSL invoice lines, the IC price list table,
Airflow Variables for the event API OAuth + schema id, and the staging
today/hist tables. This folder is a sanitized reference, not a deploy.

## Sanitization notes

- GCP projects `hd-dwh-stream-*` → `dwh_project` / `dwh_project_dev`
- Datasets `dwh_refined` / `dwh_discovery` / `dwh_trusted_staging` →
  `refined` / `discovery` / `trusted_staging`
- Source tables generalized (`odoo_wsl_invoice_lines_timestamped` →
  `odoo_wsl_invoice_lines`, `IC_price_list_tbl` → `ic_price_list`)
- Agency filter literals (`MCC%`) → `PARTNER%`
- Event API host / schema ids / OAuth Variable names generalized
- Real notification emails → `dataops@example.com`
- Owner / author names removed
- Package import `horeca_digital.*` → local `list_price_query` /
  `list_price_export`
- Production insert SQL condensed: full multi-CTE finance rules (reversed
  lines, multi-country agency filters, subscription history, discount
  splits) reduced to a readable core join; hash + outbound contract kept
- Avro schema parse moved outside the per-row loop (source parsed every row)

## Category

`odoo_integration/07-list-price-export/`
