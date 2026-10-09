# Data flow: Wholesale NL HubSpot reverse export

## Shared steps (prospects / matched / dedupe)

1. **Auth** — Password-grant OAuth2; Basic client id/secret, form
   username / password. Token cached on the client; 401 clears token
   and retries the POST once.
2. **Resolve snapshot** — Airflow Variable
   `wholesale_nl_enrichment_snapshot` suffixes the three discovery
   tables. Wrong suffix → empty or missing table; fix Variable, do not
   redeploy the DAG.
3. **Query** — Full SELECT with `ifnull` defaults. No date filter, no
   watermark.
4. **Build payload** — Row → dict; drop keys whose value is None (BQ
   nulls that survived ifnull are rare but stripped).
5. **Chunk** — Lists of 5,000 (Variable override). Each POST body is
   `{"sessionid": <int>, "records": [...]}` plus a trailing newline
   (partner contract).
6. **POST** — Sequential chunks on one OAuth client. Log response per
   chunk. Any RequestException / HTTP error fails the task.

## Task differences

| Task | Source table pattern | Endpoint Variable | Record shape |
|------|----------------------|-------------------|--------------|
| `export_data_prospects` | `…_HoReCa_prospect_{snap}` | `wholesale_nl_hubspot_export_prospect_url` | ~60 enrichment fields |
| `export_data_matched` | `…_HoReCa_matched_{snap}` | `wholesale_nl_hubspot_export_matched_url` | Same as prospects |
| `export_data_deduplication` | `…_deduplication_{snap}` | `wholesale_nl_hubspot_post_merge_url` | `id_winning`, `id_losing` |

## Failure modes

| Failure | Behaviour |
|---------|-----------|
| OAuth / API down | Task fails; siblings still run (no edges) |
| Wrong snapshot Variable | Empty export or BQ not-found → task fails |
| Mid-chunk HTTP error | Task fails after partial HubSpot session; re-trigger after CRM cleanup |
| Composer OOM | Full in-memory list for large prospect tables — raise worker mem or stream |
| Re-trigger without CRM cleanup | Possible duplicate HubSpot rows if endpoint is not idempotent |

## Upstream / downstream

Upstream: discovery enrichment tables produced after pattern 69 land +
dbt (out of scope). Downstream: HubSpot CRM for wholesale NL field
sales. No warehouse write-back from this DAG.
