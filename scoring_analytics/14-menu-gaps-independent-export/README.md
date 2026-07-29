# Pattern 14: Independent-establishment menu-gaps export

Monthly pipeline that Avro-encodes and bulk-posts independent
(non-account) menu-gap rows to a partner event API. Countries run
sequentially; within each country, five parallel tasks split rows with
`FARM_FINGERPRINT … MOD N` and stream from BigQuery.

Distinct from pattern 12 (ranked wholesale-account gaps with dbt
refresh + article/rank schema). This pattern ships *address / geo /
contact + gap* under a separate Avro contract and PII posture — no
dbt step in the export DAG.

Source (read-only):
- `dags/etl_dana_rex_menu_gaps_non_metro_export.py`
- `dags/horeca_digital/dana_rex_menu_gaps_non_metro_export.py`

## Files

| File | Role |
|------|------|
| `menu_gaps_indep_query.py` | Active ISO list + per-country SELECT / D-1 helper |
| `menu_gaps_indep_export.py` | Hash-partition query, OAuth, Avro, chunked POST |
| `dag_menu_gaps_indep_export.py` | Composer DAG: sequential countries × parallel batches |
| `BUSINESS_CASE.md` | Why a separate schema from the ranked feed |
| `ARCHITECTURE.md` | Components + Mermaid diagram |
| `DATA_FLOW.md` | Run order, idempotency, failure modes |

## Quick start

```bash
python -c "import ast; ast.parse(open('menu_gaps_indep_query.py').read())"
python -c "import ast; ast.parse(open('menu_gaps_indep_export.py').read())"
python -c "import ast; ast.parse(open('dag_menu_gaps_indep_export.py').read())"
python menu_gaps_indep_query.py    # prints a SQL prefix
python menu_gaps_indep_export.py   # Avro schema parse + partitioned SQL smoke
```

To run for real you need the refined per-country tables, event-API
OAuth Variables, and a registered independent-gaps schema id. This
folder is a sanitized reference, not a deploy.

## Sanitization notes

- GCP projects `hd-dwh-stream-*` → `dwh_project` / `dwh_project_dev`
- Dataset / tables `dwh_refined.dana_rex_menu_gaps_non_metro_{cc}` →
  `refined.menu_gaps_independent_{cc}`
- Product / partner / wholesale brand names generalized
  (`non_metro` → `independent`, event bus names generalized)
- Event API host / schema ids / OAuth Variable names externalized
- Real notification emails → `dataops@example.com`
- Owner / author names removed from DAG body
- Package imports `horeca_digital.*` → local modules
- Hardcoded schema id moved to Airflow Variables
- Avro `gdpr_info` kept honest: contact/address/geo marked as PII
  (production schema flagged `table_PII: no` despite phone/email —
  corrected here for portfolio clarity)
- Streaming + single schema parse + 401-with-payload retry preserved

## Category

`scoring_analytics/14-menu-gaps-independent-export/`
