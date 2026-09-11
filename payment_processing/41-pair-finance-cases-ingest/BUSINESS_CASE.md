# Business case: Collections partner multi-market case ingest

## Problem

Collections case files for five markets (AT, DE, FR, ES, IT) lived
behind a partner REST API. Partnership Management needed a daily
refined table for recovery performance tracking. Ad hoc pulls meant
missed markets, no idempotent re-run story, and INTEGER/STRING schema
drift when BigQuery autodetection saw case ids.

## Decision

One Composer DAG owns the multi-market contract:

1. **Per-market TaskGroup** — `extract_cases` → `load_gcs` →
   ShortCircuit `has_records` → BQ staging, so a missing key or empty
   day skips that market without failing the DAG.
2. **Secret Manager keys per market** — `collections-{market}-api-key`,
   with Variable JSON fallback for bootstrap.
3. **Idempotent GCS land** — skip extract when the date/market object
   already exists; full seed via conf `{"full_load": true}`.
4. **Explicit staging schema** — `STAGING_SCHEMA_FIELDS` keeps landing
   columns as STRING plus DATE/TIMESTAMP metadata; autodetect off so
   `case_id` never flips type.
5. **dbt behind ShortCircuit** — job id from Variable (default empty);
   markets still land even when dbt is not configured yet.
6. **SLA only on `end`** — Airflow SLA is relative to logical date;
   putting it on every task emailed skipped `load_gcs` tasks nightly.

This is inbound API → GCS → staging → dbt. It is not the payment
wallet KYC export (pattern 11) or the wallet API land (pattern 37).

## Constraints I cared about

- Markets without a provisioned key must skip, not fail the run.
- Zero-case days must not block sibling markets or the dbt gate
  (`ignore_downstream_trigger_rules=False` on ShortCircuit).
- Raw JSON preserved in `raw_json` so schema evolution is a dbt change,
  not a re-extract.
- Poll interval for dbt Cloud is amortized (timeout / 15, clamped
  60–120s) so Composer workers are not woken every few seconds.

## Outcome

Partnership Management reads one refined table. Ops has a runbook for
401 / 429 / empty extract / false SLA mail. Re-runs are delete-GCS +
clear-staging, not a rewrite of the DAG.
