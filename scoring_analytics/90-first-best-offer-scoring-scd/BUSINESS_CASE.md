# Business case: First Best Offer scoring SCD

Wholesale and independent-account teams needed a stable weekly "first
best offer" score — which product to lead with for each account — that
survived model refreshes without losing the previous recommendation.
Analysts wanted "what did we recommend last Monday?" as much as "what
do we recommend now?"

I kept scoring on a dedicated GCE VM and historization in Composer /
BigQuery. The model runtime (Python env, feature caches, vendor
artifacts) changed more often than the warehouse contract, and pinning
that stack to a VM avoided Composer worker image churn. The SCD spine
in BigQuery is what hiring managers care about here: keyhash + rowhash,
expire-then-insert, far-future `_valid_until`.

## What this unlocked

- Two populations, one DAG: wholesale account grain and independent
  establishment grain share the same historization shape
- Parallel predict on a warm VM — Monday morning wall-clock mattered
  more than serial elegance
- Downstream monthly partner export (pattern 04) could read historized
  scores without re-running the model
- Audit columns (`_job_name`, `_sourcesystem`, `_create_ts`) made
  support tickets answerable without digging through SSH logs

## Constraints

- VM start/stop is real money. `stop_vm` uses `all_done` so a failed
  predict still tears the instance down. Do not "fix" that away.
- Historization tasks also used `all_done` in production. A failed
  predict can still stage yesterday's score table and emit a green-
  looking SCD pass. Prefer `all_success` on the stage→expire→insert
  chain when you rewrite; I left the original rule so the footgun is
  visible.
- SSH via BashOperator + GCS-copied private key is operational debt.
  An SSH Connection / SSHOperator is the cleaner rewrite; the pattern
  value is the SCD contract, not the shell.
- `_rowhash` hashes the full `TO_JSON_STRING(t)` of the score row. Any
  column the model adds invalidates every row — intentional when the
  feature set is the product, expensive when someone adds a debug
  column by accident.
- Expire sets `_valid_until` to end-of-yesterday, not `CURRENT_TIMESTAMP`.
  That matches overnight warehouse conventions elsewhere in this repo
  (#67) and keeps "valid on date D" queries simple.

## What this is not

Not pattern 04 — that is the monthly multi-country FBO/NBO *export*
to a partner event bus (Avro + hash-delta). Not pattern 47 activity
scores. Not pattern 67's daily refined-zone SCD spine (different
entities, reservation-pinned night ETL). Not Next Best Offer — that
sibling DAG is the natural follow-up ship, same VM/SCD shape, different
model outputs.
