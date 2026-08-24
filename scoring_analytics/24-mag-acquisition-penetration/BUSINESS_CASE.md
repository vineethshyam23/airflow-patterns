# Business case: MAG acquisition + penetration monthly export

Finance and commercial leadership needed month-end acquisition dollars
and wholesale→platform penetration rates on the partner event bus —
without warehouse access. Upstream refine already builds the
historical MAG reporting tables; this DAG ships them.

I kept acquisition and penetration as two independent country chains
in one Composer DAG. Same schedule, same OAuth client pattern, but
different Avro contracts and different schema ids. Coupling them into
one task graph would have forced a failed acquisition market to delay
penetration for no reason. Leaving them as siblings lets Composer
interleave work and lets ops clear one chain without touching the
other.

## What this unlocked

- Deterministic monthly snapshot of acquisition + penetration per
  market (plus a corporate rollup market)
- Same OAuth / Avro / chunk pattern ops already know from other
  event-ingest DAGs
- `ALL_DONE` sequential chains so a flaky market does not strand the
  rest of the continent
- Tiny payloads (4–5 fields) — full historical reship is cheap

## Constraints

- Schedule is `45 15 2 * *`. Upstream month-end refine of
  `refined.hist_*_reporting` must finish earlier that day. Prefer an
  explicit Dataset / table sensor over "hope the cron order holds".
- Both chains use `ALL_DONE`. DAG green does **not** mean every market
  shipped — watch failed-task count.
- Event ingest is additive. Re-runs re-post the same history —
  coordinate with the consumer before a historical replay.
- Aggregate market code `ag` maps to warehouse country `corp`. Do not
  invent ISO codes for it; the partner path still uses `/ag/{schema}`.
- Penetration IFNULL(…, 0) applies to ISO markets only. The aggregate
  query historically left nulls alone — keep that divergence so a
  corp null spike is visible.

## What this is not

Not FBO/NBO scoring (pattern 04). Not establishment listing dumps
(pattern 17). Not per-customer product footprint (pattern 23). Not
the SQL that *builds* the historical MAG tables — only the export
path to the event bus.
