# Architecture: Odoo invoices partner export

One dbt refresh, then sequential per-country blocks (FR → ES).
Inside each country, five hash partitions stream BQ rows to Avro
bulk ingest. The DAG owns concurrency; the export module owns
OAuth + encode + POST.

## Diagram

```mermaid
flowchart TB
  subgraph upstream [Upstream warehouse]
    ODOO[(Odoo invoice lines)]
    DBT[dbt Cloud job odoo_invoices]
    REF[(refined.odoo_invoices)]
  end

  subgraph composer [Cloud Composer]
    START[start]
    DBT_T[dbt_odoo_invoices_refresh]
    CS_FR[start_fr]
    B0[export_fr_batch_0]
    B1[export_fr_batch_1]
    B4["export_fr_batch_2..4"]
    CE_FR[end_fr]
    CS_ES[start_es]
    BE["export_es_batch_0..4"]
    CE_ES[end_es]
    ENDN[end]
  end

  subgraph partition [Hash partition per batch]
    FP["FARM_FINGERPRINT odoo_id-invoice_id-product_code MOD 5"]
  end

  subgraph sinks [Event ingest]
    STAG[stagger_parallel_token_request]
    OAUTH[OAuth client_credentials then password]
    BULK["POST ingest_base/country/schema_id"]
  end

  ODOO --> DBT --> REF
  START --> DBT_T
  DBT_T --> CS_FR
  CS_FR --> B0
  CS_FR --> B1
  CS_FR --> B4
  B0 --> CE_FR
  B1 --> CE_FR
  B4 --> CE_FR
  CE_FR --> CS_ES --> BE --> CE_ES --> ENDN
  REF --> FP
  FP --> B0
  FP --> B1
  FP --> B4
  B0 --> STAG --> OAUTH --> BULK
  B1 --> STAG
  B4 --> STAG
  B1 --> OAUTH
  B4 --> OAUTH
  B1 --> BULK
  B4 --> BULK
```

## Components

**odoo_invoices_export**  
Builds the `FARM_FINGERPRINT … MOD N = batch` SELECT, streams the
BQ iterator, Avro-encodes each row, and POSTs chunks of 1000.
Schema is parsed once per batch. OAuth prefers
`client_credentials`, retries throttled IdP responses via
`acquire_token_with_retry`, then falls back to password grant.
401 on ingest clears the token and retries the same payload.

**event_api_auth**  
`stagger_parallel_token_request` + `acquire_token_with_retry`.
Copied from the same production helper used by other partner
exports; critical when five batch tasks wake together.

**DAG ordering**  
`start → dbt → {start_cc → [batch_0..4] → end_cc}×countries → end`.

Countries are chained (`end_fr → start_es`). Batches inside a
country fan out under `max_active_tasks=3`, so at most three of
the five shards run concurrently. That is intentional pool
tuning — not an accidental match to `TOTAL_BATCHES`.

## Failure surfaces

| Failure | Effect | Recovery |
|---------|--------|----------|
| dbt job timeout / fail | No export tasks run | Fix dbt, clear and re-run |
| IdP 403 storm | Batch tasks retry with backoff | Stagger + raise pool carefully |
| Single batch fail | Sibling batches may still finish; `end_cc` is ALL_DONE | Re-run failed task only |
| Partner 5xx | Linear backoff up to 10 attempts | Check partner status; re-run batch |
