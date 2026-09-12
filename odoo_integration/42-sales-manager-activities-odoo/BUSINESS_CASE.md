# Business case: Field-sales activities → Odoo CRM

## Problem

Field-sales managers log completed visits in a central activities API
(eight EU markets). CRM needed those visits as `crm.lead` records the
same morning — with product intent, store linkage, and UTM source —
without waiting for a weekly extract or a Salesforce round-trip.

Ad hoc pulls left gaps: one market missing, double loads on clear/re-run,
and no single place to see whether Odoo create counts matched the
warehouse lead model.

## Decision

One Composer DAG owns the contract end-to-end:

1. **OAuth2 password grant + paginated fetch** — per-country NDJSON
   land on the Composer data folder, then copy to the rawzone bucket.
2. **Country staging APPEND** — eight BigQuery tables, schema on GCS,
   so a bad file for HU does not block DE.
3. **Same-day branch skip** — if the DAG is re-queued on the same
   calendar day, skip the API pull. Accidental clear/re-run must not
   double-append staging for the same window.
4. **dbt after all countries** — build the unified lead model once;
   product-mapping snapshot reloads TRUNCATE before dbt.
5. **Odoo push + count monitor** — create leads via the shared lead
   engine (pattern 02), then compare warehouse vs Odoo counts and
   Slack the result.

This is inbound field-sales API → warehouse → CRM. It is not the
Odoo→event-bus export family (patterns 06–09) and not the lead-engine
class alone (pattern 02).

## Constraints I cared about

- Markets must fan out in parallel; one empty country must not fail
  siblings.
- Token expiry mid-pagination must refresh and retry the page, not
  abort the country.
- `load_date` follows the vendor `lastModified` convention (same day
  vs +1 catch-up) so dbt incremental keys stay stable.
- Odoo write stays behind dbt — never push raw API rows to CRM.
- Credentials only in Airflow Variables; no secrets in the repo.

## Outcome

Sales ops sees overnight visits as CRM leads before standup. Ops has a
runbook for 401 / empty country / same-day skip / count mismatch.
Re-runs are delete-staging + clear tasks, not a rewrite of the DAG.
