# Data flow: Medallia feedback ingest

## Schedule and window

| Item | Value |
|------|-------|
| Cron | `0 5 * * *` (05:00 UTC) |
| Lookback | 366 days from wall-clock `date.today()` at parse |
| API page size | 100 nodes |
| Iteration cap | 2000 |
| Catchup | False |

`LOAD_DATE` for the GCS object name is also parse-time today. A
manual clear + re-run later the same calendar day overwrites the
same CSV path; a backfill for an older `ds` still writes today's
filename unless you change that.

## End-to-end

1. **OAuth** — client_id / client_secret from Variable
   `medallia_creds`. Token file under `/tmp` survives across task
   retries on the same worker disk; treat that as convenience, not
   a security boundary.
2. **GraphQL** — `feedback(first:100, orderBy DESC e_responsedate)`
   with `fieldDataList` for the mapped attribute ids. Each node
   becomes one pandas row; values vs labels chosen per field.
3. **Hash** — key = establishment + response_date; row = all survey
   attributes including English translation columns and the custom
   parameter. Empty establishment becomes `N/A` before MD5.
4. **GCS** — headerless QUOTE_ALL CSV at
   `medallia/medallia_{LOAD_DATE}.csv` on the rawzone bucket.
5. **Trusted snapshot** — trusted →
   `trusted_staging.tmp_medallia_feedback_record` (TRUNCATE).
6. **Staging load** — CSV →
   `trusted_staging.medallia_feedback_record` (TRUNCATE).
7. **Insert** — append new/changed hash pairs into tmp with
   `_valid_flag=TRUE`, `_valid_until=2099-12-31`.
8. **Update** — set `_valid_flag=FALSE` on tmp rows still marked
   valid, inside the lookback, whose hash pair is gone from
   staging.
9. **Promote** — tmp → trusted (TRUNCATE).

## Failure modes

| Failure | Effect |
|---------|--------|
| OAuth / GraphQL error payload | Extract raises; no CSV overwrite if upload never reached |
| Empty page / missing `data` | Exception with vendor `errors` blob |
| Schema object missing on Composer | `load_staging` fails; trusted untouched until promote |
| Insert SQL typo | Append fails; tmp still holds pre-run snapshot |
| Update fails after insert | Tmp has new rows + unclosed obsolete; trusted still old |
| Worker OOM on large paginate | Extract dies mid-loop; partial CSV not uploaded |

No row-count gate between API and staging. If you need that, add a
Python XCom compare against GraphQL `totalCount` (available on the
response but unused in production).

## Field categories (~22 attributes)

| Category | Examples |
|----------|----------|
| Identity | establishment_id, country/language iso, product_name |
| NPS | nps_value 0–10, promoter/detractor reason + comment |
| Churn | initial choice, leaving reason, willingness call/stay |
| Downgrade | main reason + other comment |
| Translations | English MT columns for free-text fields |
| Keys | unique_survey_id, text_custom_parameter |

Empty-string NPS is cast to NULL then INT64 on insert.
