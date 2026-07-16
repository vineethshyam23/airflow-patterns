# Data flow: POS product category prediction

Schedule: daily around 08:00. Catchup off — we only care about current
unscored names, not backfilling every historical calendar day.

## Step 1 — Find names that still need a label

```sql
SELECT DISTINCT CONCAT(fg.name, " ", prod.name) AS product_name
FROM trusted_views.pos_product prod
LEFT JOIN trusted_views.pos_family_group fg
  USING (branch_office_id, family_group_id)
WHERE CONCAT(fg.name, " ", prod.name) NOT IN (
  SELECT DISTINCT product_name
  FROM trusted.pos_product_category_prediction
)
```

If this returns empty, the scorer exits 0 and the overview task can still
run (or you short-circuit — we usually still rebuilt the view so sales
windows stayed fresh).

## Step 2 — Load the model

Download `gs://dp_landingzone/prediction_models/food_beverage_classification.pkl`
to a worker-local path. `joblib.load` into memory. One download per DAG run.

## Step 3 — Predict

For each product name:

1. Pass `[product_name]` into `model.predict`
2. Collect `{product_name, prediction, predicted_at}`

Single-item lists match how the training pipeline was serialized. Batching
inside sklearn was possible but not worth re-validating for this volume.

## Step 4 — Write predictions

`insert_rows` against `trusted.pos_product_category_prediction` in chunks
of 10,000. Schema is intentionally thin:

| column        | type      | notes                          |
|---------------|-----------|--------------------------------|
| product_name  | STRING    | natural key / join key         |
| prediction    | STRING    | model class label              |
| predicted_at  | TIMESTAMP | run timestamp (UTC)            |

No SCD here. A name is scored once; if the model is retrained, you truncate
or delete and let the anti-join pick names up again. I preferred that over
versioning every prediction row for a label that analysts treat as current.

## Step 5 — Rebuild product overview

BigQuery job with `WRITE_TRUNCATE` into `selfservice.pos_product_view`:

1. Aggregate sales over the last ~2 months (weekly lag columns for a light
   trend signal)
2. Join outlet / family / product dimensions
3. Left-join predictions on `CONCAT(family_group, " ", product)`
4. Drop test outlets

Analysts query the overview table; they do not call the model.

## Idempotency

Re-running the scorer on the same day is safe: the anti-join skips names
already present. Re-running the overview always replaces the self-service
table — that is expected.

## Typical volumes (order of magnitude)

After the initial backlog clear, daily unscored names were usually low
hundreds to low thousands. Overview rebuild dominated wall time, not the
Python scoring loop.
