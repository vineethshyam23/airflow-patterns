# Data Flow: Daily refined-zone SCD spine

## Happy path

1. **Trigger** — cron `15 3 * * *` (03:15 UTC daily).
2. **Actuals** — sequential WRITE_TRUNCATE for each SCD entity from
   `refined.vw_analytical_{entity}_actual` →
   `refined.analytical_{entity}_actual`.
3. **Hist insert** — after each entity's actual lands, WRITE_APPEND
   new `_keyhash|_rowhash` pairs from
   `trusted_staging.vw_analytical_{entity}_hist`.
4. **Hist expire** — deferrable UPDATE closes current rows missing
   from staging (`_valid_flag=FALSE`, `_valid_until` = end of yesterday).
5. **Test quarantine** — after RT establishments actual, WRITE_TRUNCATE
   `refined.derived_test_establishments` with conservative + progressive
   rows from Hydra and Reservation Tool trusted views.

## Paths

| Path | What moves | Write mode |
|------|------------|------------|
| A Actuals | refined views → analytical_*_actual | WRITE_TRUNCATE |
| B Hist insert | staging hist view → analytical_*_hist | WRITE_APPEND (new hash pairs only) |
| C Hist expire | UPDATE analytical_*_hist | DML; deferrable |
| D Test list | trusted Hydra/RT → derived_test_establishments | WRITE_TRUNCATE |

## SCD hash contract

```
current in hist  = rows where _valid_flag = TRUE
new version      = staging hash pair NOT IN current hist
expired version  = current hist hash pair NOT IN staging
```

Staging views own hash computation (typically MD5 of business key +
payload columns). Composer never recomputes hashes — it only compares
concatenated pairs. That keeps the DAG thin and puts schema evolution
in dbt / view SQL where analysts already look.

## Failure modes

| Failure | Effect | Recovery |
|---------|--------|----------|
| Actual WRITE_TRUNCATE fails | Downstream hist for that entity blocked; later actuals may still run if parallelized — here they are sequential so the chain stops | Fix view / slots; clear failed + downstream |
| Hist insert fails after actual | Actual table fresh; hist missing new versions | Re-run insert → expire only |
| Hist expire fails after insert | New versions current; stale versions still `_valid_flag=TRUE` (two currents for one key) | Re-run expire; investigate before full hist rebuild |
| Reservation Variable wrong | Jobs run on-demand; cost spike / slot contention with BI | Fix `bq_night_etl_reservation`; no data rewind needed |
| Progressive regex too broad | Real establishments excluded from Order / mapping | Tighten progressive branch; re-TRUNCATE test table |
| Prior run still active at 03:15 | `max_active_runs=1` blocks | Wait or mark failed after investigation |

## Upstream / downstream

**Upstream (must exist before 03:15):**
- Refined actual views (`vw_analytical_*_actual`)
- Staging hist views with `_keyhash` / `_rowhash`
- Trusted Hydra + Reservation Tool establishment views

**Downstream:**
- MAG monthly hist (#64 / #65) reading analytical actuals / related refined
- Salesforce refined aggregates (#59)
- Order / mapping DAGs anti-joining `derived_test_establishments`
- Partner exports that assume current hist `_valid_flag=TRUE`

## Distinct from sibling SCD / hist patterns

| | 01 Matching SCD | 61 Derived events | 64/65 MAG hist | 67 (this) |
|---|-----------------|-------------------|----------------|-----------|
| Grain | Match pair | Event append | Month × country | Daily entity version |
| Mechanism | SCD2 builders | LAG / unnest | WRITE_APPEND calendar | Hash insert + expire |
| Schedule | Partner / job | Daily subset | 1st / 2nd of month | Nightly 03:15 |
| Test quarantine | No | No | No | Yes (dual mode) |
| Reservation pin | No | No | No | Yes |
