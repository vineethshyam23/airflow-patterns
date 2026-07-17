# Pattern 03: Adyen payment terminal integration

Nightly sync of Adyen Management API inventory (merchants, stores, terminals,
settings) into BigQuery, then a small write-back loop that reassigns terminals
and patches default settings using a trusted match table.

Source (read-only):
- `dags/etl_adyen_payment_terminal.py`
- `dags/horeca_digital/adyen_payment_terminal_integration.py`

## Files

| File | Role |
|------|------|
| `adyen_payment_terminal.py` | Management API client, pagination, reassign + settings PATCH |
| `dag_adyen_payment_terminal.py` | Composer DAG: extract → GCS → BQ staging → dbt → management API |
| `BUSINESS_CASE.md` | Why pull + controlled write-back beat manual portal work |
| `ARCHITECTURE.md` | Components + Mermaid diagram |
| `DATA_FLOW.md` | Run order, XCom use, failure behavior |

## Quick start

```bash
python -c "import ast; ast.parse(open('adyen_payment_terminal.py').read())"
python -c "import ast; ast.parse(open('dag_adyen_payment_terminal.py').read())"
```

To actually run anything you need Adyen Management API credentials in an
Airflow Variable (`adyen_payment_terminal_creds`), a Composer bucket for the
JSONL landing zone, raw GCS + staging tables, and the dbt job that builds the
serial↔store match model. This folder is a sanitized reference, not a deploy.

## Sanitization notes

- GCP projects `hd-dwh-stream-*` → `dwh_project` / `dwh_project_dev`
- Raw buckets → `dp_rawzone` / `dp_dev_rawzone`
- Staging dataset `dwh_trusted_staging` → `trusted_staging`
- Match table → `trusted.int_payment_terminal_erp_serial_store_match`
- Real notification emails → `dataops@example.com`
- Owner / author names removed
- Custom `DbtCloudRunJobOperator` + hard-coded job id replaced with a Variable-driven stub (`trigger_dbt_adyen_models`)
- Package import `horeca_digital.*` → local `adyen_payment_terminal`
- Inline `AirflowException` imports moved to module top

No credentials were in the source files; still, never commit real Adyen keys.

## Category

`payment_processing/03-adyen-payment-terminal/`
