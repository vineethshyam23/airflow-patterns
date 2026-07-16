# Pattern 02: POS product category prediction

Daily batch scoring of POS product names into food/beverage categories using
a sklearn model stored in GCS, orchestrated on Cloud Composer.

Source (read-only): `dags/horeca_digital/posms_predict_product_category.py`
plus the archived DAG wrapper that rebuilt the product overview after scoring.

## Files

| File | Role |
|------|------|
| `predict_product_category.py` | Unscored-name query, GCS model load, predict, BQ insert |
| `dag_product_category_prediction.py` | Airflow DAG: score then rebuild self-service overview |
| `BUSINESS_CASE.md` | Why batch text classification beat manual tagging |
| `ARCHITECTURE.md` | Components + Mermaid diagram |
| `DATA_FLOW.md` | Step-by-step run behavior and idempotency |

## Quick start

```bash
# Read-only dry look at the scoring path
python -c "import ast; ast.parse(open('predict_product_category.py').read())"

# In a Composer-like env with GCP creds + the real pickle:
# python predict_product_category.py
```

You need a BigQuery project, the trusted POS views, a prediction table, and
the model artifact in GCS before this does anything useful. The code here is
a sanitized reference, not a drop-in deploy.

## Sanitization notes

- GCP project `hd-dwh-stream-1` → `dwh_project`
- Landing bucket renamed to `dp_landingzone`
- `fposms_*` / `posms_*` tables → `pos_*` equivalents under `trusted_views` / `trusted` / `selfservice`
- Real notification emails → `dataops@example.com`
- Composer plugin paths kept generic; adjust `SCRIPT_PATH` for your image
- Model filename shortened (dropped dated suffix); treat GCS object as versioned externally

Business metrics and team sizes were not invented. Volumes in DATA_FLOW are
order-of-magnitude from how this class of job behaved, not contractual SLAs.

## Category

`ml_pipelines/02-product-category-prediction/`
