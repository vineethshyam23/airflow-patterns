# Business case: Hydra Adobe traffic CSV publish

Product and web ops teams need daily, file-shaped traffic metrics for
the Hydra website — mail/phone clicks, opening-times views, visits by
device and locale — without granting BigQuery access to every
downstream consumer. This DAG turns refined Adobe/Hydra tables into
dated CSVs in a dedicated publish bucket.

## Why CSV publish instead of a shared BQ view

The consumers are not warehouse users. They pull flat files on a
schedule, expect stable filenames, and treat the bucket root as a
drop zone. A view or scheduled query still leaves them writing their
own extract tooling. Publishing the files ourselves keeps the
contract in one place: five tables, two trailing days, overwrite on
rerun, 60-day retention.

## Why this sits after refined, not on the raw Data Feed

Patterns 49 and 51 land Adobe Data Feed hits into staging/trusted.
Traffic marts (`site_traffic_*`) are already shaped for reporting —
device labels, locale grains, click types. Re-deriving those from
hit-level TSV in a second producer would duplicate mart logic and
drift from the warehouse source of truth. Read refined, publish CSV.

## Production constraints that shaped the design

**Idempotent overwrite.** Consumers re-read the same date files. We
delete matching target objects for the window before copy so partial
reruns do not leave stale siblings with alternate date formats
(`2025-1-5` vs `2025-01-05`).

**Staging then publish.** Writer rights on the consumer bucket are
narrower than Composer. Stage under `data/site-traffic/` on the
Composer bucket, copy only successful extracts, then delete staging
objects that actually landed. Failed copies keep staging for retry.

**Empty is not failure.** A metric table with no rows for a day is
`no_data`, not an error — the other tables still publish. On-call
reads the extract summary counts, not a red DAG for a quiet Monday.

## Tradeoffs I would still revisit

`START_DATE` / `END_DATE` are computed at module import with
`datetime.now()`, not `{{ ds }}`. That breaks clean backfills and can
shift if the scheduler reparses on a different calendar day. A
logical-date rewrite is the first fix if we need historical republish.

Extractions run sequentially inside one PythonOperator with pandas
in memory. Fine for current volumes; large days would be better as
`EXPORT DATA` or mapped tasks per table/date. Full-bucket list for
retention and overwrite also scales poorly — prefix per metric would
be cheaper once object counts grow.
