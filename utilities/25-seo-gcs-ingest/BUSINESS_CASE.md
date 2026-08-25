# Business case: SEO business-listing GCS ingest

Vendor SEO dumps arrive as gzipped NDJSON drops — sometimes scheduled,
sometimes ad-hoc after a country refresh. We needed a landing path that
preserves the vendor bytes, stages an uncompressed loadable copy, truncates
into BigQuery staging, runs dbt, and only then archives the staging objects.

I kept promote / load / dbt / archive as a single linear chain with
`max_active_runs=1`. Parallelizing across files looks tempting, but the
load is a wildcard `WRITE_TRUNCATE` into one staging table — racing two
runs would silently clobber each other. Manual schedule (`None`) matches
how the dumps actually arrive: irregular vendor drops, not a clock.

## What this unlocked

- One place ops can drop a dump (`uploads/`) and trigger Composer
- Content + GCS md5 metadata on every promoted object — enough to debug
  "did we already load this file?" without a side table
- Compression detected from magic bytes, not filename — vendor renames
  stop breaking the pipeline
- Dry-run CLI (`scan` / `ingest` / `archive`) so you can preview stems
  and metadata before `--apply`
- Archive happens *after* dbt succeeds — a failed refine does not bury
  the only loadable copy under `archive_ingested/`

## Constraints

- Load is `WRITE_TRUNCATE` on `staging.seo_establishments`. This is a
  full-replace staging hop; dbt is what merges into refined history.
  Do not treat staging as durable.
- `max_active_runs=1` is load-bearing. Do not raise it unless the load
  disposition changes.
- Schema JSON lives in the rawzone bucket (`schema_json/...`), not in
  the ingest bucket. Wrong `schema_object_bucket` fails the load after
  a successful promote — fixable, but confusing in the Graph view.
- dbt job id comes from Airflow Variables. Unset in DEV → EmptyOperator
  keeps the graph shape without calling dbt Cloud.
- Ingest always processes every `uploads/` candidate (no content-md5
  skip). Dedup across re-drops is a staging/dbt concern, not a promote
  filter.

## What this is not

Not menu URL extraction (pattern 18) — that starts from *refined*
listings and crawls HTML. Not Freshdesk API landing (pattern 19). Not
the dbt models that build refined SEO tables — only the GCS → staging
→ dbt trigger → archive path.
