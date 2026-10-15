# Data flow: Dining Guide DQ-gated publish

## Happy path

1. **07:30 UTC** — DAG starts. Deferrable dbt Cloud job builds
   `trusted.dining_guide_data_base` from ERP + market + product models.
2. **Run id capture** — parallel branch stores the dbt run id in
   Variable `etl_dining_guide_dbt_runids` for ops lookup.
3. **Hash** — worker pulls distinct `establishment_id`, applies
   Blake3→base64url (10 chars), WRITE_TRUNCATEs
   `trusted.dining_guide_establishment_id_hash`.
4. **Compare** — FULL JOIN today's trusted (test=0) to yesterday's
   `dining-guide-prod.app_data.dining_guide_data_base`. One monitoring
   row of boolean degradation flags (15% threshold).
5. **Gate** — if any flag TRUE (or query errors): Slack, stop.
   Else: approve → CREATE OR REPLACE backup of prior trusted push.
6. **Publish** — for each `[stage, project]` in
   `dining_guide_publish_targets`, SELECT trusted ⟕ hash as `dine_id`
   into `app_data.dining_guide_data_base` (WRITE_TRUNCATE).
7. **Ratings (dev)** — optional SQL file load into
   `dining_guide_data_base_with_ratings` on the configured dev project.

## Failure modes

| Failure | Behaviour |
|---------|-----------|
| dbt job fails / times out (60m) | Hash/compare do not run; no publish |
| Hash worker OOM | Task fails; no compare/publish |
| Monitoring query errors | Gate returns fail → Slack, no publish |
| Any 15% flag TRUE | Slack with monitoring table pointer; trusted build kept |
| Single env publish fails | Upstream backup already done; retry that env task |

Blocked publishes leave the previous day's app tables intact
(WRITE_TRUNCATE never starts). The dbt trusted table still reflects
today's build — useful for forensics, not rolled back.

## Column contracts (publish SELECT)

Public `dine_id` is the hashed uid. CRM account/establishment ids,
wholesale + product customer flags, geo, cuisine, order/reservation
URLs and counts, and `menu_url` ride along. Test rows are included
when `test_establishment IS NOT NULL` so app envs can exercise fixtures.

## Operational notes

- Re-run after fixing upstream: gate re-compares against still-stale
  prod until a successful publish lands.
- To force a known-good cutover after an intentional market change,
  temporarily drop noisy flags from `GATE_FLAG_COLUMNS` (production
  commented Instagram when SEO coverage cratered) — document the
  exception in the monitoring Slack thread.
- Sibling manual full-load DAG exists in production; not shipped here.
