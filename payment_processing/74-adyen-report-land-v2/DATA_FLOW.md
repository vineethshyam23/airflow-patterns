# Data flow: Adyen report land v2

## Daily run (05:30 UTC)

1. `start` → `ensure_dbt_source_tables`  
   Reads three schema JSON objects from the rawzone bucket and runs
   `CREATE TABLE IF NOT EXISTS` for settlement, received payments, and
   platform margins staging tables. Never truncates existing data.

2. Three parallel branches after ensure:

   **Settlement**  
   `fetch_settlement_details` probes
   `settlement_detail_report_{ds}_{ds}.csv` on Adyen CA. On hit: stamp
   `file_name`, upload as `adyen/report_v2/settlement_details/{ds}/settlement_details.csv`.
   On miss: ShortCircuit (return falsy).  
   `load_staging_settlement_details` WRITE_TRUNCATEs staging from that object.

   **Received payments**  
   Same shape; candidate names try underscore then hyphen date forms.
   Stable object: `received_payment_details.csv` under the day prefix.

   **Platform margins**  
   `check_platform_margins_upload` lists `*.csv` under the analytics drop
   prefix. Empty → ShortCircuit. Otherwise `stage_platform_margins_with_file_name`
   clears the day rawzone prefix, stamps each basename as `file_name`,
   uploads per-file CSVs, then wildcard-loads into staging. Drop objects
   are left for the v1 DAG to archive.

3. `stage_1` (`none_failed_min_one_success`) joins the three load paths.
   At least ensure (and any successful branch) must succeed; skipped
   ShortCircuit branches do not fail the join.

4. Optional `adyen_report_v2_run_dbt_job` when
   `adyen_report_v2_dbt_job_id` is non-empty (deferrable poll). Else
   `stage_1` → `end`.

## Incremental contract (dbt, not this DAG)

Staging is a daily truncate-reload of yesterday's file. Trusted int models
are incremental; unique keys documented in the helper module:

- Settlement: `Psp_Reference`, `Type`, `Modification_Reference`, `Batch_Number`
- Received: `Psp_Reference`
- Margins: `Billing_Month`, `AccountHolder`, `BalanceAccount`, `Fee_Type`,
  `Fee_Name`, `Currency_Code`

## XCom

Fetch / check tasks return download metadata or CSV object lists. Margins
staging pulls the check task's object list via `source_task_id`. Return
values are not shown in logs for the Adyen fetch tasks
(`show_return_value_in_logs=False`) to keep CA filenames out of noisy UI
noise — they still land in XCom for operators that need them.

## Failure / skip summary

| Condition | Outcome |
|-----------|---------|
| Report not published yet | Branch ShortCircuit; run continues |
| Margins drop empty | Margins ShortCircuit; settlement/received unaffected |
| Load fails after successful fetch | Branch fails; join may still proceed if another branch succeeded |
| Ensure fails | All branches blocked; run fails |
| dbt fails | Land already committed; fix models and re-run dbt or clear task |
