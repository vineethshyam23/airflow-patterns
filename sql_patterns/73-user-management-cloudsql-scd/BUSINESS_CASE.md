# Business case: User Management Cloud SQL SCD Type 2

Identity and access for a multi-product restaurant platform live in a
dedicated MySQL database — users, roles, personas, and establishment
assignments. Downstream analytics, CRM reverse syncs, and entitlement
checks need a historically correct warehouse copy, not just today's
snapshot.

## Problem

A nightly truncate-reload loses "who had access when." Role churn and
establishment reassignment are operational facts: support tickets,
audit questions, and partner scoring all ask for as-of state. Pulling
identity ad hoc from the OLTP instance is expensive and collides with
Cloud SQL Admin's one-operation-at-a-time limit.

## Approach

Full CSV export of each UM table into a dual-bucket path (instance
export bucket → DWH raw zone), load to staging with an inline schema,
then hash-based SCD Type 2 into `um_*` trusted tables. Surrogate keys
are namespaced at extract time so warehouse joins never confuse UM ids
with Odoo / Salesforce integers.

Exports run serially with a schedule-aware retry operator: the same
MySQL instance takes backups in a known window, and 409
`operationInProgress` is the normal failure mode under load.

## Tradeoffs

| Choice | Why | Cost |
|--------|-----|------|
| SCD in the DAG (not dbt) | Matches the Offer Tool / food-order generation of lands; no extra job id | ~9 tasks × N tables; harder to unit-test SQL |
| Full daily dump | Identity tables are small vs POS/order; watermark complexity not worth it | Longer Cloud SQL export wall clock |
| Semicolon CSV + inline schemas | Matches European MySQL export settings already used on the instance | Schema drift requires DAG edits |
| First-run bootstrap | Cold start used to fail mid-SCD when trusted was missing | Extra Python task per table |
| `email_on_failure=False` in prod | Alert noise during early rollout | Silent failures — monitor externally |

I would move SCD2 to dbt on a greenfield rewrite. In this fleet the
pattern stayed in Composer because the sibling lands (#27 / #56) already
did, and operators knew how to debug INSERT/UPDATE hashes in the UI.

## What this is not

Not Hydra (#39) — that is a weekly CMS full dump with historization in
dbt. Not Reservation Tool (#54) — id watermarks for append-heavy OLTP.
Not Keycloak SSO land (#57) — that is backup-tar event append, not UM
RBAC dimensions.
