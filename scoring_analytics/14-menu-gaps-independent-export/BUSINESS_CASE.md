# Business case: Independent-establishment menu-gaps export

Field and partner teams also care about establishments that are *not*
on the wholesale account book — independent restaurants where the gap
signal is "this menu is missing X" plus where to find the place, not
"attach article Y to account Z with rank N".

Pattern 12 already ships the ranked, account-linked feed. This sibling
ships a different Avro contract: address, geo, contact, cuisine, and
the gap itself. Same Composer concurrency model (sequential countries,
five hash batches), different grain and GDPR posture.

I left dbt out of this DAG on purpose. The independent refined tables
are lighter and owned upstream; bolting another Cloud job here would
couple two refresh cadences for no operational win.

## What this unlocked

- Monthly partner feed for independent (non-account) menu gaps without
  granting warehouse access
- Stable parallelization via
  `FARM_FINGERPRINT(establishment_id || menu_item || ingredient) MOD N`
  — no batch column on the refined table
- Streaming BQ → Avro → POST so a fat market does not OOM the worker
- Clear separation from the ranked wholesale feed so schema evolution
  on one side does not force a re-register on the other

## Constraints

- Schedule is monthly (`30 6 1 * *`) but the export filter is D-1.
  Same caveat as pattern 12: if the business expects a full month of
  deltas, expose `full_load` via a DAG Param — do not silently change
  the filter.
- Active country list starts narrow (one market in production). Extending
  it is a config change *plus* a refined table and schema registration —
  do not assume ISO codes alone are enough.
- Chunk size is 1000. Address/contact rows are lighter than ranked
  article rows (2000 in pattern 12) but the payload still carries
  phone/email — keep body size under the API limit.
- Avro schema marks contact/address/geo as PII. Treat re-runs and
  historical replays as a privacy event, not just a bus replay.
- `end` / `end_{country}` use `ALL_DONE`. A failed batch does not block
  later markets — monitor failed-task count.

## What this is not

Not the ranked wholesale menu-gaps export (pattern 12). Not FBO/NBO
scoring (pattern 04). Not the matching-engine export (pattern 10).
Not the upstream SQL that builds the independent refined tables.
