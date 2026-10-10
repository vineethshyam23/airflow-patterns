-- Dev-only ratings-enriched publish target.
-- Production loads a much larger self-contained establishment query
-- (~1k lines) into app_data.dining_guide_data_base_with_ratings.
-- Placeholders {dwh_project} are filled by the DAG before submit.
-- Contract: base row + website attribute array + rating structs.

SELECT
  b.*,
  h.hashed_uid AS dine_id,
  ARRAY<STRUCT<section STRING, attribute STRING>>[] AS wb_attributes,
  STRUCT(
    b.rating_google AS google_score,
    CAST(NULL AS INT64) AS google_review_count,
    CAST(NULL AS FLOAT64) AS reservation_food_avg,
    CAST(NULL AS INT64) AS reservation_review_count
  ) AS ratings
FROM `{dwh_project}.trusted.dining_guide_data_base` b
LEFT JOIN `{dwh_project}.trusted.dining_guide_establishment_id_hash` h
  ON b.establishment_id = h.establishment_id
WHERE b.test_establishment IS NOT NULL
