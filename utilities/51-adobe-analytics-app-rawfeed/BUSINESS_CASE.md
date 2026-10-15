# Business Case: Adobe Analytics app Data Feed land

Mobile product needed the same hourly warehouse presence the web suite
already had — but Adobe ships a separate report suite, a different
landing prefix, and a hit schema dominated by app id / device / screen
events rather than browser and referrer. Cloning the web lander and
parameterizing the suite stem looked tidy on a whiteboard; in practice
the graphs diverged enough that a sibling DAG was cheaper to operate
than a shared template with growing `if suite == app` branches.

I kept a second Composer lander (instead of folding into #49) for three
production reasons:

1. **Suite isolation is an ops boundary** — a bad web drop must not
   block app staging TRUNCATE, and vice versa. Separate
   `max_active_runs=1` graphs and processed prefixes keep incidents
   contained.
2. **Lookup cardinality is not a config knob** — web fans out thirteen
   dimensions; app only needs connection, country, language, OS.
   Parallelizing unused browser/plugin/search-engine loads on every
   app hour was wasted quota and false failure surface.
3. **Refined SQL ownership differs** — app projection is eVar-heavy
   (app version, screen, event_name, device brand) and joins lookup
   keys as strings. Web keeps the large status-code decode + IP hash
   path. A shared SQL file would have become a merge hazard.

## What this unlocks

- Trusted app hit append (`aa_app_hit_data`) for audit / replay.
- Truncated mobile lookup dims (`aa_app_country`, `aa_app_operating_systems`, …).
- Refined `analytics_datafeed_app` with visit/hit/visitor IDs, device
  brand normalization, OS aggregate, and visit first/last hit windows.
- A clean hand-off point for the later dbt-owned app transform job
  (not shipped here — thin trigger wrapper).

## Tradeoffs I accepted

- `max_bad_records=1000` on the hit TSV (tighter than web's 50k). App
  feeds are narrower; a looser budget hid schema drift we actually
  wanted to see.
- Refined APPEND without the one-day `hit_id` anti-join that #49 uses.
  Source left that predicate commented while the lander was still
  stabilizing. Documented as a known gap — enable it if Adobe
  redeliveries double-count before transforms move to dbt.
- Languages lookup is loaded to trusted but not joined in the refined
  projection that shipped. Kept the load so trusted stays complete
  relative to the feed dump; refined can pick it up later without
  re-landing.
- Worker local disk under `/tmp/gcs_extracted_app/` (separate from web)
  so concurrent suite hours do not collide on extract folders.

## Not this pattern

- Pattern 49: web-suite hourly Data Feed land (full lookup fan-out +
  hit_id dedupe window).
- Pattern 28: AppFigures weekly mobile *store* analytics (different
  vendor, API not Data Feed).
- `etl_aa_adobe_rawfeed_app_job.py`: dbt Cloud trigger sibling — skip
  unless orchestration is non-thin.
