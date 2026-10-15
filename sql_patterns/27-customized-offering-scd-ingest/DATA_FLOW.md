# Data flow: Offer Tool SCD ingest

## Schedule

| Item | Value |
|------|-------|
| Cadence | Daily `15 6 * * *` (06:15 UTC) |
| `max_active_runs` | 1 |
| Catchup | False |
| Retries | 1 × 3 minutes |

## Per-table grain

| Stage | Object | Write mode |
|-------|--------|------------|
| Cloud SQL export | `gs://db-export-offer-tool-prod/offer-tool/{table}/{date}/{hour}/{table}_prod.csv` | new object |
| Raw zone copy | same path under `dwh-rawzone` | overwrite object |
| Staging | `dwh_project.trusted_staging.ot_{table}_prod` | WRITE_TRUNCATE |
| Tmp snapshot | `dwh_project.trusted_staging.tmp_ot_{table}_prod` | WRITE_TRUNCATE then APPEND / UPDATE |
| Trusted | `dwh_project.trusted.ot_{table}_prod` | WRITE_TRUNCATE from tmp |

Valid rows carry `_valid_flag=True`, `_valid_from` hour-truncateded,
`_valid_until=2099-12-31`. Expired rows close `_valid_until` one
second before the new version's `_valid_from`.

## Tables in this pattern (15)

`data_product_need`, `restaurant_contact_event`, `followup_reminder`,
`error_report`, `file`, `product_recommendation_state`,
`product_suggested_alternative`, `restaurant_comment`,
`restaurant_comment_file`, `restaurant_custom_menu`,
`restaurant_custom_menu_file`, `user`, `restaurant_segment_update`,
`restaurant_details_update`, `google_places`.

## Ordering

1. Export table N (waits on export N-1)
2. Copy CSV to raw zone
3. Snapshot trusted → tmp
4. Load CSV → staging
5. Insert new hash pairs into tmp
6. Expire superseded OfferTool rows in tmp
7. Promote tmp → trusted

Steps 2–7 for table N do not wait on table N+1's export.

## Failure modes

| Failure | Effect | Ops response |
|---------|--------|--------------|
| Cloud SQL export IAM / quota | That table's chain fails; later exports still schedule (`all_done`) | Fix IAM, clear-and-rerun failed subtree |
| Schema JSON mismatch on load | Staging load fails; trusted unchanged for that table | Align `schema_json/ot_*_prod.json`, re-run |
| Hash column drift (query change) | Mass expire + insert looks like full rewrite | Diff export SQL vs prior deploy before merging |
| Overlapping runs | Blocked by `max_active_runs=1` | Leave as-is; do not raise concurrency on Cloud SQL |
| Parse-time path date on long retry | CSV path may not match export URI hour | Prefer re-run from failed export; rewrite paths to `{{ ds }}` when touching the DAG |

## Downstream (out of scope here)

Trusted `ot_*` tables feed a separate zone / Elasticsearch refresh
DAG and analytics models (Food Graph masterdata, Adobe COF reports).
Those DAGs are not part of this pattern folder.
