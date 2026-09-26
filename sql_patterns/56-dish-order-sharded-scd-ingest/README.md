# Pattern 56: Food-ordering multi-shard Cloud SQL SCD Type 2 ingest

Daily land of a multi-tenant food-ordering OLTP schema from **dozens of
MySQL Cloud SQL shards** into BigQuery trusted with Type 2
historization. The reusable piece is the **discover → filter-by-IP →
parallel shard export → CSV merge → hash SCD** orchestration — not the
restaurant domain itself.

Distinct from pattern 27 (single Offer Tool instance, sequential
`CloudSQLExportInstanceOperator` dumps). Distinct from pattern 39
(Hydra weekly full dump, no SCD in the DAG) and pattern 54 (Reservation
Tool id-watermark + Sunday full sync; historization lives in dbt).

Source (read-only):
- `dags/etl_dishorder.py`
- Composer-mounted bash scripts under `/home/airflow/gcs/data/.../scripts/`
  (reconstructed here as sanitized stubs — not in the airflow2 Git tree)

## Files

| File | Role |
|------|------|
| `dag_dishorder_sharded_scd.py` | Discover → shard fan-out → merge → SCD chains |
| `shard_config.py` | Trimmed shard map + master/sharded table lists |
| `bq_reservation.py` | Night-ETL BigQuery reservation wrapper |
| `scripts/*.sh` | getdbs / per-shard export / master export / merge / clean |
| `BUSINESS_CASE.md` | Why shard fan-out + merge + hash SCD |
| `ARCHITECTURE.md` | Components + Mermaid diagram |
| `DATA_FLOW.md` | Run order, object layout, failure modes |

## Quick start

```bash
python -c "import ast; ast.parse(open('shard_config.py').read())"
python -c "import ast; ast.parse(open('bq_reservation.py').read())"
python -c "import ast; ast.parse(open('dag_dishorder_sharded_scd.py').read())"
python -c "from shard_config import SHARD_INSTANCES, ALL_TABLES; print(len(SHARD_INSTANCES), len(ALL_TABLES))"
bash -n scripts/getdbs.sh && bash -n scripts/dishordersplitted.sh && bash -n scripts/dishordermerge.sh
```

To run for real you need Cloud SQL Admin export IAM on master + every
shard, GCS write on the export bucket and raw zone, schema JSON objects
under `schema_json/order_*.json`, and the bash scripts mounted where
`SCRIPTS_ROOT` points. This folder is a sanitized reference, not a deploy.

## Sanitization notes

- Product name `DISH Order` / project ids → `food-order` / `food-order-prod` /
  `dwh_project`
- Buckets `hd-digital-test` / `hd-digital-dp-rawzone` →
  `db-export-food-order-prod` / `dwh-rawzone`
- Datasets `dwh_trusted*` → `trusted` / `trusted_staging`
- 44 production shards → 4 representative entries in `SHARD_INSTANCES`
- ~38 tables → 4 master + 12 sharded in `shard_config.py`
- Real emails / owner names removed; `email_on_failure` turned on
- Deprecated `airflow.contrib.*` operators → providers Google transfers
- `DummyOperator` → `EmptyOperator` for sharded download skips
- `ReservedBigQueryInsertJobOperator` kept as a local thin wrapper
- Bash scripts were Composer data-folder artifacts; stubs document the
  contract without shipping production `gcloud` credentials or IPs

## Distinct from nearby patterns

| | 27 Offer Tool | 39 / 54 | 56 (this) |
|---|---|---|---|
| Source shape | 1 Cloud SQL instance | 1 instance | Master + N shards |
| Export API | CloudSQLExportInstanceOperator | Admin CSV (+ retry op) | Bash `gcloud sql export` |
| Merge step | None | None | Per-table shard CSV concat |
| Historization | SCD2 in DAG | Full dump / watermark → dbt | SCD2 in DAG (`_keyhash`/`_rowhash`) |

## Category

`sql_patterns/56-dish-order-sharded-scd-ingest/`
