# Business case: SEO listing GCS ingest

We buy multi-country business-listing dumps from an SEO vendor.
They arrive as large gzip NDJSON blobs — sometimes misnamed
(`.json` with a gzip body), sometimes plain. Ops need a boring
path from "file landed in a bucket" to "rows in staging + dbt
models refreshed", without writing a one-off notebook every time
a dump shows up.

I kept the Composer DAG thin: two Python callables for bucket
hygiene, one `GCSToBigQueryOperator` for the load, one dbt job,
done. The interesting engineering lives in `gcs_ingest.py` —
stream decompress with stdlib gzip (Composer / urllib3 gotchas
with SDK gzip transcoding), content-md5 metadata for later
dedupe, and a dry-run CLI so ops can preview before `--apply`.

## What this unlocked

- Deterministic object names:
  `{YYYYMMDD}_countries-{n}_{raw_md5[:8]}` — readable in the
  console, unique enough without a central registry
- Four-prefix contract (`uploads` → `stg_to_load` + `archive_raw`
  → load → `archive_ingested`) so raw vendor bytes survive even
  if staging is truncated on the next run
- Manual schedule (`None`) — dumps are irregular; polling burned
  Composer minutes for empty `uploads/`
- Same helpers runnable from CLI outside Composer for incident
  recovery

## Constraints

- `WRITE_TRUNCATE` on staging means the load expects the full
  dump set currently in `stg_to_load/`. Do not leave orphan stage
  files from a half-failed prior run without checking.
- Archive runs *after* dbt. If dbt fails, stage objects stay put —
  re-run from the load or dbt task, do not re-ingest.
- Compression is detected from magic bytes, not the filename.
  Trust that; vendor uploads have lied about extensions.
- `max_active_runs=1` — overlapping ingests would race on the
  same stage prefix.

## What this is not

Not menu URL extraction (pattern 18). Not the dbt models that
turn staging into refined country tables. Not a scheduled crawl
of the vendor API — this is file-drop ingress only.
