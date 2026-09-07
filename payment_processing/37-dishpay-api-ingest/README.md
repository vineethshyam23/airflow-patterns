# Pattern 37: Payment wallet API ingest (KYC + transactions + VOP)

Daily inbound pipeline that OAuth-pulls three payment-wallet DWH
feeds, lands NDJSON on Composer → rawzone, loads BigQuery staging,
and triggers dbt Cloud jobs for trusted models. Per-feed ops
notifications include API record counts (and loaded counts for
transactions).

Distinct from pattern 11 (outbound KYC Avro export to a partner event
bus) and pattern 03 (Adyen terminal Management API). This pattern is
the *source-of-truth land* for payment-product tables.

Source (read-only):
- `dags/etl_dishpay_dbt.py`
- `dags/horeca_digital/get_dish_pay_data.py`

## Files

| File | Role |
|------|------|
| `payment_api.py` | OAuth client, three pagination styles, NDJSON write, loaded-count query |
| `dag_dishpay_api_ingest.py` | Composer DAG: fan-out fetch → branch → staging → dbt → notify |
| `BUSINESS_CASE.md` | Why extract and dbt share one DAG |
| `ARCHITECTURE.md` | Components + Mermaid diagram |
| `DATA_FLOW.md` | Run order, date coupling, failure modes |

## Quick start

```bash
python -c "import ast; ast.parse(open('payment_api.py').read())"
python -c "import ast; ast.parse(open('dag_dishpay_api_ingest.py').read())"
python payment_api.py
```

To run for real you need `payment_wallet_creds`, DEV/PROD GCP
connections, Composer bucket Variable, dbt Cloud job ids, and the
Slack webhook connection (optional — falls back to print). This folder
is a sanitized reference, not a deploy.

## Sanitization notes

- GCP projects `hd-dwh-stream-*` → `dwh_project` / `dwh_project_dev`
- Rawzone buckets → `rawzone` / `rawzone_dev`
- Dataset / tables `dwh_trusted_staging.dishpay_*` →
  `trusted_staging.payment_*`
- Product paths `dish-pay/` → `payment-wallet/`
- Airflow Variable `dishpay_creds` → `payment_wallet_creds`
- dbt job numeric ids → Variables
  `payment_wallet_dbt_kyc_job_id` / `payment_wallet_dbt_transactions_job_id`
- Slack channel / username generalized; emoji stripped from messages
- Real notification emails → `dataops@example.com`
- Owner / author names removed from DAG body
- Package imports `horeca_digital.*` → local `payment_api`
- Removed module-level `print(get_loaded_data_count())` (would hit BQ on import)
- Removed commented `__main__` block that contained live-looking OAuth examples
- Token value no longer logged on refresh retry
- `DbtCloudRunJobOperator` / Slack provider optional stubs for reference checkouts
- `max_active_runs=1` on the DAG constructor

## Category

`payment_processing/37-dishpay-api-ingest/`
