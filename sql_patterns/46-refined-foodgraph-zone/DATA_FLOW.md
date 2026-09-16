# Data Flow: Food Graph refined zone

## Schedule

`45 5 * * *` (05:45 UTC daily), `max_active_runs=1`, `catchup=False`.

Upstream wholesale / card country jobs typically finish 01:00–02:15
UTC. The gap is deliberate buffer, not idle waste — large DE/FR
trusted loads overrun often enough that a 03:00 start was painful.

## Path A — per-country analytics chain

For each of 16 ISO codes:

1. **masterdata** — establishments with wholesale card as data source.
2. **assortments** — food / drink / disposable articles from analytical
   article tables.
3. **txn_for_analytics** — join transactions to assortments.
   AT is special-cased: no `var_tu_key` equality (legacy assortment
   keying). Everyone else requires art_no + var_tu_key.
4. Fan-out from txn into three analytics grains:
   - **article** — last purchase per customer / article / month +
     purchase frequency window.
   - **visit** — recent visit totals (top 1000 dates); production
     nests line-item JSON for richer markets.
   - **branch_topseller** — top 1000 articles by revenue with buyer
     penetration, optionally remapped onto peer segments.

## Path B — global fan-in

Each country txn / article / visit / topseller task also points at a
matching global truncate job. Those jobs are `UNION ALL` across the
dense Western / CEE set (AT, DE, FR, HR, HU, NL, PL, PT, RO). Markets
in the 16-country loop but outside that set still get country tables;
they simply do not enter the global rollup.

## Path C — loop1 → partitioned history

After **all** country txn tasks succeed, `loop1_done` fires. Then for
each of ~11 markets:

```
txn_for_analytics_{ISO}
  INNER JOIN wholesale_to_dwh_id_mapping
  → all_available_transactions_{ISO}
     PARTITION BY date_of_day (DAY)
     CLUSTER BY dwh_id
```

WRITE_TRUNCATE rebuilds the full history each day. Cost follows the
largest markets; correctness follows the barrier.

## Path D — other globals

| Task | Trigger | Role |
|------|---------|------|
| earliest_visit_for_analytics | after global txn fan-in | Materialize view → table |
| earliest_visit_per_customer | root / loose in prod | Materialize view → table |
| analytics_topseller | after earliest_visit | Materialize view → table |
| analytics_pwg | root | Materialize view → table |
| masterdata_for_customized_offerings | root | DE+PL customer dedupe for COP |

Portfolio keeps COP contact columns out of the SELECT list; production
carried owner / LR email and phone for the offering tool.

## Failure and recovery

| Symptom | Likely cause | Action |
|---------|--------------|--------|
| `loop1` never clears | One country txn failed or queued | Fix that ISO first; do not clear barrier manually |
| Partitioned table empty for ISO | Mapping join miss or empty txn | Check `wholesale_to_dwh_id_mapping` coverage |
| Slot exhaustion / long queue | Reservation unset or undersized | Set `foodgraph_bq_reservation`; avoid overlapping runs |
| Global UNION schema break | New column on one country table only | Align country SELECTs before fan-in |

## Downstream consumers

- Customized offering zone (pattern 27 sibling DAG) — visit / article /
  COP masterdata.
- Food Graph ML propagation (pattern 40) — refined assortments and
  transaction context, not this DAG's schedule.
- Benchmarking / Metro analytics dashboards — global rollups.
