# Architecture: dbt Cloud run status Slack digest

Three Python tasks in a linear chain: discover run ids from Variables,
resolve status via dbt Cloud, post failures to Slack. No BigQuery,
no GCS, no dbt job submission.

## Diagram

```mermaid
flowchart TB
  subgraph triggers [Trigger DAGs - out of scope]
    T1["Product DAG A\nsubmit dbt Cloud job"]
    T2["Product DAG B\nsubmit dbt Cloud job"]
    V["Airflow Variables\nkeys containing dbt_runids\nJSON list of run ids"]
    T1 --> V
    T2 --> V
  end

  subgraph compose [Composer DAG dbt_jobs_status_dag]
    G["get_dbt_runids\ncreate_session scan"]
    S["get_dbt_job_status\nDbtCloudHook get_job_run"]
    N["slack_notification\nwebhook per Failed"]
    G --> S --> N
  end

  subgraph dbtc [dbt Cloud]
    API["account jobs / runs API"]
  end

  subgraph slack [Slack]
    CH["#dwh-dbt-ops"]
  end

  V --> G
  S --> API
  N --> CH
```

## Components

**get_dbt_runids**  
Opens an Airflow DB session, lists Variable keys containing
`dbt_runids`, deserializes each as JSON, flattens with `sum(..., [])`.
`JSONDecodeError` is swallowed per Variable so one corrupt entry does
not blank the whole poll.

**get_dbt_job_status**  
For each run id: `DbtCloudHook.get_job_run`, then `get_job` for the
human name (falls back to numeric `job_id`). Maps
`is_success / is_error` to Succeeded vs Failed. Returns only Failed
rows on XCom.

**slack_notification**  
If the Failed list is empty, no-op. Otherwise instantiates
`SlackWebhookOperator` per row and `.execute({})` inside the
callable — same delivery shape as pattern 83.

## Design notes

Variable-key convention over a registry table keeps this DAG from
depending on warehouse freshness. The tradeoff is discoverability:
ops must know the naming rule, and stale run ids linger until a
trigger overwrites them. If you outgrow Variables, land run ids into
a small BQ control table and poll that instead — keep the Failed-
only Slack contract.
