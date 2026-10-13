# Business case: Adyen report land v2

Finance and commercial analytics need settlement journals, received-payment
details, and platform fee margins from the Adyen Customer Area for a POS
payment partner. Those reports are published as dated CSV files — not as a
streaming webhook — and arrive on different cadences (daily settlement /
received; margins via an analytics drop folder).

## Problem

v1 merged multi-day settlement ranges in the Composer worker. Settlement
days are tens of MB; stretching a backfill window OOMed the worker and
left DA staring at staging tables without a reliable file lineage column.
Empty report days also broke dbt when the staging table had never been
created.

Pattern 03 already covers terminal inventory via the Management API. It
does not land settlement economics.

## Approach

v2 runs on a one-day window (`ds` = previous calendar day at 05:30 UTC),
probes Adyen for that day's settlement and received-payment filenames,
uploads each hit as its own rawzone object, stamps `file_name` onto every
row, and loads with schema JSON from the rawzone. Platform margins take a
separate ShortCircuit path from an analytics drop bucket — stamped and
loaded without deleting the drop (v1 still archives).

Before any fetch, `ensure_dbt_source_tables` runs `CREATE TABLE IF NOT
EXISTS` from the same schema JSON. Skipped ShortCircuit branches no longer
leave dbt pointing at a missing relation. dbt (stg → int) owns incremental
unique keys; the DAG only lands yesterday's truncate-reload staging slice.

## Tradeoffs

| Choice | Why | Cost |
|--------|-----|------|
| 1-day window, no merge | Avoids worker OOM; matches Adyen publish lag | Backfills need many DAG runs or a separate range helper |
| ShortCircuit on 404 / empty drop | Empty days are normal, not failures | Join uses `none_failed_min_one_success` |
| `file_name` in staging + trusted | DA debugs to the CA basename without touching staging | Schema JSON must include the column |
| Ensure tables before fetch | dbt sources exist even when every branch skips | Extra BQ DDL task every run |
| Leave margins drop for v1 | Dual-run cutover without racing deletes | Temporary dual ownership of the drop folder |
| dbt job id from Variable | DEV has no job; PROD can rotate ids | Empty Variable skips dbt silently |

I kept CA report download in Python rather than a vendor connector: the
filename probe rules (underscore vs hyphen received-payment names, stable
`dest_object_name` for single-day loads) are the actual contract, and they
change slower than the rest of the Adyen API surface.

## What this is not

Not pattern 03 — Management API merchants/stores/terminals + write-back.
Not pattern 11 / 37 — wallet KYC / txn API lands. Not pattern 41 —
collections partner case files. Not a webhook ingest.
