# Business case: dbt Cloud run status Slack digest

Composer kicks dozens of dbt Cloud jobs from different DAGs. Each
trigger path has its own failure email, but nobody watches every
inbox at 05:00 — and a green Composer task that only *submitted* a
job is not the same as a green dbt Cloud run. I needed one morning
poll that answers: which dbt runs we kicked yesterday are actually
red in dbt Cloud?

The contract is deliberately thin. Trigger DAGs write run ids into
Airflow Variables (keys containing `dbt_runids`). This DAG scans those
Variables, calls dbt Cloud for each id, filters to Failed, and posts
to a dbt ops Slack channel. No warehouse table, no dashboard, no
ownership of job submission.

## What this unlocked

- A single Slack surface for dbt Cloud failures that cut across
  product DAGs — matching engine, MCC country jobs, Adobe feeds, etc.
- Decoupling: trigger DAGs stay focused on submit + Variable write;
  this poller owns "did it finish red?"
- Cheap operability: Variable discovery via `create_session` means
  adding a new trigger DAG is a naming convention (`*dbt_runids*`),
  not a code change here
- Failed-only noise control — Succeeded runs never hit the channel

## Constraints

- Relies on trigger DAGs writing valid JSON lists. Bad JSON is
  skipped; missing Variables mean silent empty mornings.
- dbt Cloud account id is hardcoded (sanitized as `DBT_ACCOUNT_ID`).
  Multi-account tenants need a Variable.
- SlackWebhookOperator runs inside the Python callable. Airflow does
  not retry each webhook as its own task.
- Polling is not streaming: a job that fails and gets re-run before
  05:01 may never appear if the Variable was overwritten with the
  new run id.

## What this is not

Not pattern 80 (Slack when parallel BigQuery materializations fail
inside Composer). Not pattern 83 (daily BQ spend leaderboard). Not a
dbt Cloud job trigger wrapper — those thin `DbtCloudRunJobOperator`
DAGs are out of scope for this pattern. Not a dbt Patterns repo
artifact; this is the Composer-side ops digest only.
