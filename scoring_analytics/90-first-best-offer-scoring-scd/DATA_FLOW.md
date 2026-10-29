# Data flow: First Best Offer scoring SCD

## Run order

1. Monday 02:00 UTC — Composer starts `etl_first_best_offer`.
2. `start_vm` boots the scoring GCE instance.
3. Two BashOperators SSH in parallel:
   - wholesale model → `refined.fbo_wholesale_score`
   - independent model → `refined.fbo_independent_score`
4. Per arm (after its predict task):
   - stage → `trusted_staging.tmp_fbo_*_score` (WRITE_TRUNCATE)
   - expire changed rows in `refined.fbo_*_score_historized`
   - insert new versions (WRITE_APPEND)
5. `stop_vm` runs when both predicts finish or fail (`all_done`).

## Paths

```
GCE VM predict (wholesale)
  → refined.fbo_wholesale_score
  → staging + SCD audit cols (_keyhash on wholesale_account_id)
  → expire hist where key matches and rowhash differs
  → append new keyhash|rowhash versions

GCE VM predict (independent)
  → refined.fbo_independent_score
  → staging + SCD audit cols (_keyhash on establishment_id)
  → expire / insert (same shape)
```

## SCD contract

| Step | Predicate |
|------|-----------|
| Expire | `_keyhash` in staging AND `concat(_keyhash,_rowhash)` not in staging |
| Insert | `concat(_keyhash,_rowhash)` not among currently-valid hist rows |
| Open version | `_valid_until = 2099-12-31`, `_valid_flag = TRUE` |
| Closed version | `_valid_until = end of yesterday`, `_valid_flag = FALSE` |

Unchanged rows (same keyhash and rowhash) are neither expired nor
re-inserted. Brand-new keys insert only. Content changes expire the
old version then insert the new one.

## Idempotency

- Re-running after a successful Monday is mostly a no-op: staging
  rebuilds identical hashes, expire matches nothing, insert finds
  existing valid rows.
- Re-running after a mid-chain failure is trickier: expire may have
  already closed rows. Safe recovery is "re-run predict → stage →
  expire → insert" once; do not manually re-open hist rows.
- `max_active_runs=1` prevents overlapping Monday executions.

## Failure modes

| Failure | Effect | What to do |
|---------|--------|------------|
| Predict fails, stage still `all_done` | Stale scores historized | Flip stage chain to `all_success`; clear hist if polluted |
| VM left running | Cost leak | Confirm `stop_vm` `all_done`; check GCE console |
| SSH StrictHostKeyChecking / key rotate | Predict red | Refresh GCS key object; prefer SSHOperator next |
| Model adds a column | Full-table SCD churn | Expected if the feature is product; otherwise drop column |
| Downstream #04 reads mid-expire | Rare race on Monday | Export schedule is monthly; usually fine |

## Scale notes

Score tables are account/establishment grain — typically low hundreds
of thousands of rows, not billions. Parallel arms matter for wall
clock of the model, not for BigQuery slot pressure. If a third
population appears, add an `FboScoreArm` + predict job; do not copy
the six BQ operators by hand.
