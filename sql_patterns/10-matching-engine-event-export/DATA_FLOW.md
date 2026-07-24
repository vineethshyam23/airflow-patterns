# Data flow: Matching engine export to partner event bus

Schedule: `0 8 * * 4` (Thursday 08:00 UTC). Catchup off — a missed
week is an explicit re-trigger, not a backfill storm.

## Stage A — Staging rebuild

`prepare_matching_staging` runs `matching_export_prepare`:

1. For each country in the prepare map, query:
   - `trusted.match_result` (valid rows, quality ≤ 150, two request types)
   - `trusted_mcc.amcc_{suffix}_customer_unique` (store / cust no / status)
   - `refined.customer_base_establishment` (product flags + dates)
2. Unpivot product flags → one row per active service
3. Mark inactive wholesale ids with service code + 700
4. Dedup: highest activity score, then oldest `date_from`
5. Replace `refined.partner_matching_export`

If prepare fails, all ingest tasks stay blocked. That is correct:
shipping last week's staging labeled as this week's refresh is worse
than a delayed partner feed.

## Stage B — Per-country Avro ingest

| Task | Source filter | Sink |
|------|---------------|------|
| `ingest_{country}` | `LOWER(country) = '{country}'` on staging | `POST /ingestbulk/{country}/{schema_id}` |

Countries in the DAG when this shipped: hr, cz, fr, de, hu, it, pl,
pt, es, nl, ro, tr, ua. Prepare SQL also covers be/sk; those markets
were not on the ingest list (schema registration / consumer scope).

Each task: SELECT → Avro encode → POST chunks of 500.

## Idempotency and re-runs

- Re-run after prepare success: re-posts the full country snapshot.
  Safe if the bus upserts on natural keys; coordinate otherwise.
- Re-run only one ingest task: fine — siblings are independent after
  prepare.
- Re-run prepare alone: replaces staging; ingest must follow or the
  bus still holds the previous week's payload.
- Never put ingest before prepare. You will ship stale staging rows.

## Failure modes worth knowing

- OAuth 401 mid-chunk: client clears token and retries the POST once
  with the same body (production dropped the body on retry — fixed
  here).
- Empty country result: often "no valid matches under the quality
  cutoff" rather than a broken pipeline — check match_result freshness
  before paging.
- Prepare OOM / long runtime: full multi-country pandas reshape in one
  task. Split by region only if weekly runtime becomes the SLA risk.
- `to_gbq` replace mid-failure: ingest must not start until the task
  succeeds — the DAG chain enforces that.
- Decimal / date Avro encoding: partner schema uses logical types;
  passing raw Python floats/strings will fail schema validation at the
  bus (production was inconsistent here — sanitized path encodes
  explicitly).
