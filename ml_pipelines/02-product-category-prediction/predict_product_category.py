"""
Batch product-category prediction for POS catalog items.

Pulls product names that have not been scored yet, downloads a sklearn
model from GCS, predicts food/beverage class, and writes results to BigQuery
in chunks.

In production this ran under Composer as a BashOperator task calling a
script on the worker. Kept here as a plain module so the scoring path is
easy to read without Composer path quirks.
"""

from datetime import datetime, timezone

import joblib
from google.cloud import bigquery, storage

# --- config (sanitized) -----------------------------------------------------
PROJECT_ID = "dwh_project"
SOURCE_PRODUCT = f"{PROJECT_ID}.trusted_views.pos_product"
SOURCE_FAMILY = f"{PROJECT_ID}.trusted_views.pos_family_group"
PRED_TABLE = f"{PROJECT_ID}.trusted.pos_product_category_prediction"

GCS_BUCKET = "dp_landingzone"
MODEL_BLOB = "prediction_models/food_beverage_classification.pkl"
# Composer workers had a writable local path for the pickle download
LOCAL_MODEL_PATH = "/tmp/pos_product_category_model.pkl"

INSERT_CHUNK_SIZE = 10_000

# Only score names we have not already written. Product name is family group
# + product, which is how cashiers often label items in the POS.
UNSCORED_PRODUCTS_SQL = f"""
SELECT DISTINCT CONCAT(fg.name, " ", prod.name) AS product_name
FROM `{SOURCE_PRODUCT}` prod
LEFT JOIN `{SOURCE_FAMILY}` fg
  USING (branch_office_id, family_group_id)
WHERE CONCAT(fg.name, " ", prod.name) NOT IN (
    SELECT DISTINCT product_name
    FROM `{PRED_TABLE}`
)
"""


def get_unscored_product_names(client: bigquery.Client | None = None) -> list[str]:
    client = client or bigquery.Client(project=PROJECT_ID)
    rows = client.query(UNSCORED_PRODUCTS_SQL).result()
    return [row["product_name"] for row in rows]


def load_model_from_gcs(
    bucket_name: str = GCS_BUCKET,
    blob_path: str = MODEL_BLOB,
    local_path: str = LOCAL_MODEL_PATH,
):
    """Download the pickle once per run. Model file is versioned in GCS."""
    storage_client = storage.Client(project=PROJECT_ID)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    blob.download_to_filename(local_path)

    with open(local_path, "rb") as fh:
        return joblib.load(fh)


def predict_categories(product_names: list[str], model=None) -> list[dict]:
    """
    Score one name at a time.

    The production classifier expected a single-item list (text feature
    pipeline), not a batch vector. I left that shape alone — changing it
    meant re-validating the model contract for little gain on daily volume.
    """
    if model is None:
        model = load_model_from_gcs()

    predicted_at = datetime.now(timezone.utc)
    output = []
    for product_name in product_names:
        # model.predict takes an iterable of strings for this pipeline
        prediction = model.predict([product_name])
        output.append(
            {
                "product_name": product_name,
                "prediction": prediction[0],
                "predicted_at": predicted_at,
            }
        )
    return output


def insert_predictions(rows: list[dict], client: bigquery.Client | None = None) -> None:
    """Streaming insert in chunks — insert_rows is fine for daily incremental volume."""
    if not rows:
        return

    client = client or bigquery.Client(project=PROJECT_ID)
    table = client.get_table(PRED_TABLE)

    for start in range(0, len(rows), INSERT_CHUNK_SIZE):
        chunk = rows[start : start + INSERT_CHUNK_SIZE]
        errors = client.insert_rows(table, chunk)
        if errors:
            # Fail loud; partial inserts are worse than a retry for this table
            raise RuntimeError(f"BigQuery insert_rows failed: {errors[:3]}")


def run() -> int:
    names = get_unscored_product_names()
    if not names:
        print("No unscored products — nothing to do")
        return 0

    print(f"Scoring {len(names)} products")
    predictions = predict_categories(names)
    insert_predictions(predictions)
    print(f"Wrote {len(predictions)} rows to {PRED_TABLE}")
    return len(predictions)


if __name__ == "__main__":
    run()
