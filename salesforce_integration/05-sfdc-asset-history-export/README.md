# Pattern 05: Salesforce asset history delta export

Daily pipeline that snapshots CRM asset/install rows from a refined
Salesforce history table, detects hash-based deltas against yesterday's
history, and pushes only changed rows as Avro events to an external ingest
API.

Source (read-only):
- `dags/horeca_digital/dana_sfdc_asset_query.py`
- `dags/horeca_digital/dana_sfdc_asset_export.py`
- `dags/horeca_digital/archived/etl_dana_SFDC_asset_history_export.py`

## Files

| File | Role |
|------|------|
| `sfdc_asset_query.py` | SQL builders + keyhash/rowhash delta helpers |
| `sfdc_asset_export.py` | OAuth client, Avro encode, chunked bulk POST |
| `dag_sfdc_asset_history_export.py` | Composer DAG: today truncate → ingest → hist append/expire |
| `BUSINESS_CASE.md` | Why daily hash-delta beats full CRM reload |
| `ARCHITECTURE.md` | Components + Mermaid diagram |
| `DATA_FLOW.md` | Run order, idempotency, failure modes |

## Quick start

```bash
python -c "import ast; ast.parse(open('sfdc_asset_query.py').read())"
python -c "import ast; ast.parse(open('sfdc_asset_export.py').read())"
python -c "import ast; ast.parse(open('dag_sfdc_asset_history_export.py').read())"
python sfdc_asset_query.py   # prints insert/send query previews
```

To run for real you need the refined SFDC asset history table, Airflow
Variables for the event API OAuth + schema id, and the staging today/hist
tables. This folder is a sanitized reference, not a deploy.

## Sanitization notes

- GCP projects `hd-dwh-stream-*` → `dwh_project` / `dwh_project_dev`
- Datasets `dwh_recommender_vertex` / `dwh_trusted_staging` →
  `refined` / `trusted_staging`
- Source table `dish_sfdc_asset_history_ES` → `sfdc_asset_history_ES`
- SFDC custom fields `Metro_Id__c` / `MetroAccountIdentifier__c` / `Store__c`
  → `Crm_Metro_Id` / `Crm_Account_Identifier` / `Store_Id`
- Event API host / schema ids / OAuth Variable names generalized
- Real notification emails → `dataops@example.com`
- Owner / author names removed
- Package import `horeca_digital.*` → local `sfdc_asset_query` /
  `sfdc_asset_export`
- Inline credential literals were already Variable-backed; still, never
  commit real client secrets

## Category

`salesforce_integration/05-sfdc-asset-history-export/`
