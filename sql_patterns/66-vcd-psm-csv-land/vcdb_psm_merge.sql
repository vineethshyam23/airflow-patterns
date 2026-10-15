-- Optional MERGE into a trusted_source grain table.
-- Production commented this step out of the land DAG after moving
-- transform work into the dbt Cloud job. Kept here as the intended
-- country + psm_month natural-key contract if you re-enable an inline
-- BQ merge instead of (or before) dbt.
--
-- Params expected by Jinja: target, source, country_code, psm_month

MERGE INTO `{{ params.target }}` AS target
USING (
  SELECT
    *,
    '{{ params.country_code }}' AS country_code,
    '{{ params.psm_month }}' AS psm_month
  FROM `{{ params.source }}`
) AS source
ON (
  target.country_code = source.country_code
  AND target.psm_month = source.psm_month
  AND target.month = source.month
  AND target.year = source.year
  AND target.Quarter = source.Quarter
)
WHEN NOT MATCHED BY TARGET THEN
  INSERT (
    month,
    uplift,
    Revenue,
    year,
    date,
    Quarter,
    country_code,
    psm_month
  )
  VALUES (
    source.month,
    source.uplift,
    source.Revenue,
    source.year,
    source.date,
    source.Quarter,
    source.country_code,
    source.psm_month
  )
WHEN MATCHED THEN
  UPDATE SET
    target.month = source.month,
    target.uplift = source.uplift,
    target.Revenue = source.Revenue,
    target.year = source.year,
    target.date = source.date,
    target.Quarter = source.Quarter,
    target.country_code = source.country_code,
    target.psm_month = source.psm_month
;
