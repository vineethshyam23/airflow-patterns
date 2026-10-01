# Data flow: Derived events change-detection

## Destination schema

All tasks append the same grain into `trusted.derived_events`:

| Column | Role |
|--------|------|
| `id_type` | Entity key namespace (table.id or analytics surrogate type) |
| `id` | Numeric entity id (`-1` for Adobe when only `idchar` applies) |
| `idchar` | String / surrogate key |
| `event` | Human-readable event name |
| `derived_event_int` | Numeric code (often `1`, or Adobe event key) |
| `derived_event_timestamp` | When the change / hit occurred |
| `derived_event_string` | Payload (new value, JSON config, Custom Event label) |
| `days_after_creation` | Days from entity creation to event |
| `_sourcesystem` | Source label |
| `_rowhash` | MD5 for dedupe |

## Run order (subset)

```
cms_modification_date
  → cms_loc_name_change
  → adobe_datafeed
  → rt_user_logindate
  → rt_auto_arrivals
  → rt_reservationchannels_change
```

Production continues with ~25 CMS, 1 Adobe, ~31 Reservation tasks in
one linear chain. Independent groups could run in parallel; they were
not parallelized historically.

## Deduplication

**SCD paths** hash:

```
MD5(id_type | id | event | derived_event_int | timestamp | string | _sourcesystem)
```

then `NOT IN (SELECT _rowhash FROM derived_events WHERE event = …)`.

**Adobe** hashes hit id + event fragments. Do not assume the SCD formula
when replaying Adobe only.

**Channels** additionally keep `record_rank_per_day = 1` so multiple SCD
versions on the same calendar day collapse to the latest.

## Idempotency / re-run

WRITE_APPEND + hash anti-join makes a full DAG re-run safe for already
landed events. A failed mid-chain run leaves earlier categories written;
retrying the DAG (or clearing failed tasks) fills the gap without
duplicating completed hashes.

## What this subset does not include

- Remaining CMS content / branding / gallery events
- Remaining Reservation menus / notifications / reservables
- Commented-out `event_rt2` additional-info task
- Parallel TaskGroups or per-source DAG split (documented as the rewrite
  direction, not implemented here)
