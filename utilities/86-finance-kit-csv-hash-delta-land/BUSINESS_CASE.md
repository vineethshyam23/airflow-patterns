# Business case: Finance-kit CSV land + hash-delta

A cost-accounting / BWA toolkit dumped ten related tables as nightly
semicolon CSVs. Finance analytics needed them in the warehouse the
same night, without waiting on a dbt Cloud job slot or teaching every
entity a separate SoftSensor + SCD2 model.

I shipped this as a pure-operator fan-out: same five tasks per entity,
hash-delta INSERT into trusted, landing delete only after append.
That kept Composer as the contract with the vendor drop folder and
kept dbt out of the critical path for a feed that was already
pre-hashed (`_keyhash` / `_rowhash` in the CSV schema).

## What this unlocked

- One DAG owns the land contract for ten entities — add a stem to
  `ENTITIES` and a schema JSON, not a new DAG file
- Staging is always today's full file (WRITE_TRUNCATE); trusted only
  grows by unseen hash pairs — cheap re-runs when the vendor re-drops
  an identical file
- Landing cleanup after trusted append means a failed insert leaves
  the drop file for retry instead of silently losing the night's data
- Date-partitioned rawzone path (`entity/YYYY-MM-DD/entity.csv`) gives
  ops a re-load handle without digging through landing

## Constraints

- `GCSListObjectsOperator` is named like a sensor but is not one.
  Empty prefix returns success with `[]`; the copy task is what fails
  when the vendor is late. Prefer `GCSObjectExistenceSensor` (or a
  short poke) if you need the DAG red before load.
- File date uses `{{ tomorrow_ds }}` with a `23:30` schedule — the
  overnight drop is labeled for the next calendar day. Backfills need
  that macro (or a Variable override), not `ds`.
- Hash-delta is append-only. Changed rows with a new `_rowhash` append
  a second version; nothing closes the prior `_valid_until`. Treat
  the validity columns as audit stamps, not a finished SCD2 spine.
- Schema JSON lives in the load bucket (`schema_json/finance_kit_*.json`).
  Missing schema fails the GCS→BQ load; keep schemas versioned with
  the DAG.
- Ten parallel chains share no barrier. One entity can go green while
  another is still red — fine for independent tables, noisy if
  finance expects an all-or-nothing night.

## What this is not

Not SEO listing NDJSON land (pattern 25) — that is a four-prefix
archive contract with stream decompress. Not POS HMAC store-details
(pattern 35) — that pulls CSV over a signed API and hands off to dbt.
Not the daily refined-zone SCD spine (pattern 67) — that closes prior
versions with LAG/hash SCD2 across many domains. This folder is the
*vendor multi-entity CSV land with in-DAG hash dedupe* that stayed
in Composer because the feed already carried the hashes.
