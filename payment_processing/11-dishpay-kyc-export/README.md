# Pattern 11: Payment KYC export to partner event bus

Daily pipeline that refreshes a refined KYC table via dbt Cloud
(staging filter → SCD2 → current valid rows), then Avro-encodes and
bulk-posts to a partner event API for a pilot market.

Distinct from pattern 03 (Adyen Management API terminal inventory /
write-back). This pattern ships *KYC onboarding status*, not terminal
hardware state.

Source (read-only):
- `dags/etl_dana_dishpay_kyc_export.py`
- `dags/horeca_digital/dana_dishpay_kyc_export.py`
- `dags/horeca_digital/dana_dishpay_kyc_query.py`

## Files

| File | Role |
|------|------|
| `kyc_query.py` | SELECT shaped for Avro string/long fields |
| `kyc_export.py` | OAuth client, Avro encode, chunked bulk POST |
| `dag_dishpay_kyc_export.py` | Composer DAG: dbt refresh → country ingest |
| `BUSINESS_CASE.md` | Why dbt + ingest share one DAG |
| `ARCHITECTURE.md` | Components + Mermaid diagram |
| `DATA_FLOW.md` | Run order, idempotency, failure modes |

## Quick start

```bash
python -c "import ast; ast.parse(open('kyc_query.py').read())"
python -c "import ast; ast.parse(open('kyc_export.py').read())"
python -c "import ast; ast.parse(open('dag_dishpay_kyc_export.py').read())"
python kyc_query.py          # prints a SQL prefix
python kyc_export.py         # Avro schema parse smoke check
```

To run for real you need the refined KYC table, the dbt Cloud job id
in `dbt_job_payment_kyc_export`, event-API OAuth Variables, and a
registered schema id. This folder is a sanitized reference, not a deploy.

## Sanitization notes

- GCP projects `hd-dwh-stream-*` → `dwh_project` / `dwh_project_dev`
- Dataset / table `dwh_refined.dana_dishpay_kyc` → `refined.payment_kyc_export`
- Product / brand names generalized (payment-product KYC)
- Event API host / schema ids / OAuth Variable names generalized
- Real notification emails → `dataops@example.com`
- Owner / author names and internal ticket ids removed from DAG body
- Package imports `horeca_digital.*` → local modules
- `DummyOperator` → `EmptyOperator` (with fallback)
- `max_active_runs` moved to DAG constructor (was incorrectly in default_args)
- Avro schema parse moved outside the per-row loop
- HTTP errors now raise (`raise_for_status`)
- 401 retry now re-sends the original payload
- DAG graph wired with `chain(start, dbt, *ingest, end)`

## Category

`payment_processing/11-dishpay-kyc-export/`
