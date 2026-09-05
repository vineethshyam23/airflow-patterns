# Business case: POS vendor store-details HMAC CSV ingest

POS and CRM reporting need a daily establishment master for one POS
vendor estate — customer / debtor ids, address, and which product
modules are active (front office, EFT, webshop, terminals, and so on).
The vendor exposes that as a single authenticated CSV webservice, not
as a warehouse-friendly API. The job is to land today's full snapshot
into staging every morning so dbt and downstream POS / Salesforce jobs
can join on a stable table.

I kept the graph linear: fetch → Composer data volume → rawzone → CSV
repair in GCS → BigQuery staging (TRUNCATE) → one dbt Cloud job. Full
reload is cheap at this volume and avoids incremental merge bugs when
the vendor silently drops or renames a flag column.

## What this unlocked

- One 07:00 UTC cron replaces manual CSV downloads from the vendor portal
- Header contract fails the fetch before any GCS or BigQuery write when
  the vendor adds / removes a column
- Address-field commas that break column counts are repaired twice
  (local write + GCS pass) so BigQuery load does not die on "Too many
  values"
- Staging isolate keeps a bad load off trusted until dbt succeeds

## Constraints

- Auth is HMAC-MD5 over `getStoreDetails` + `YYYYMMDD`. Yesterday's
  signature will not work today — fine for a daily cron, awkward for
  historical replay (you cannot re-auth past days without vendor help).
- File paths use `date.today()` at parse / fetch time. Stable for the
  scheduled run; broken for backfills. Documented, not silently rewritten.
- Short rows are padded at the end only. If the vendor omitted a middle
  column, padding misaligns — we log and investigate rather than guess.
- Repair runs at fetch and again in GCS. Redundant but cheap insurance
  after the Composer → rawzone copy; consolidate if you rewrite.

## What this is not

Not payment transaction ingest (see DishPay KYC export, pattern 11, or
Adyen terminals, pattern 03). Not the dbt models that build the trusted
view. Not a change-data feed — full daily snapshot only.
