# Pattern 55: Menu Engineering VM Postgres land

Composer DAG that lands a product Postgres database living on a GCE
VM (Dockerised, not Cloud SQL) into BigQuery staging via a
**dual-bucket** handoff: SSH `COPY` → product GCS → DWH rawzone →
TRUNCATE load → one dbt Cloud job. VM cleanup (logs + CSVs) runs
after the product upload so disk does not grow across days.

Distinct from patterns 20–22 (Deepideas partner Avro exports of
enrichment attributes). Distinct from Hydra (#39) and Reservation
Tool (#54): those use Cloud SQL Admin; this path is SSH + docker
exec because the source was never Cloud SQL.

Source (read-only):
- `dags/etl_deepideas_to_DWH.py`
- (supporting context only, not shipped) `dags/horeca_digital/deepideas_data_exporter.py`

## Files

| File | Role |
|------|------|
| `dag_menu_engineering_vm_land.py` | SSH export → dual GCS → BQ → dbt |
| `table_catalog.py` | Trimmed Postgres table list + naming constants |
| `BUSINESS_CASE.md` | Why dual-bucket + VM cleanup beat a single hop |
| `ARCHITECTURE.md` | Components + Mermaid diagram |
| `DATA_FLOW.md` | Run order, object layout, failure modes |

## Quick start

```bash
python -c "import ast; ast.parse(open('table_catalog.py').read())"
python -c "import ast; ast.parse(open('dag_menu_engineering_vm_land.py').read())"
python -c "from table_catalog import POSTGRES_TABLES; print(len(POSTGRES_TABLES), POSTGRES_TABLES[0])"
```

To run for real you need an SSH connection to the product VM with
docker+gcloud on the PATH, read on the product bucket + write on the
DWH rawzone for the Composer SA, schema JSON under `schema_json/`,
and Variables for buckets / project / optional dbt job id. This
folder is a sanitized reference, not a deploy.

## Sanitization notes

- Product / GCP names (`deepideas-*`, `metro_menu_engineering`,
  `hd-dwh-stream-1`, `hd-digital-dp-rawzone`) → Variables +
  `menu_engineering` / `dwh_project` / `dwh-rawzone` placeholders
- VM instance / zone / project ids → out of the daily DAG (runbook);
  SSH via `me_vm_ssh_conn_id`
- Hardcoded dbt job id → Variable `me_dbt_job_id` (EmptyOperator stub
  when unset)
- Owner / author names and real emails removed
- Table catalog trimmed from ~30 production tables to 12
  representative specs (country addresses, dims, import facts)
- One-time SSH keygen / VM metadata tasks left as documentation only
  (commented in source; not re-shipped as live tasks)
- Old paramiko exporter module not included — production path is
  SSHOperator, not the incomplete SFTP helper
- `max_active_runs=1` preserved

## Distinct from nearby patterns

| | 20–22 (Avro export) | 39 / 54 (Cloud SQL) | 55 (this) |
|---|---|---|---|
| Source | BQ enrichment tables | Cloud SQL MySQL | GCE VM + Docker Postgres |
| Hard part | Avro / partner contract | 409 retry + watermark / full dump | Dual-bucket IAM + VM disk hygiene |
| Land style | Event bus | Admin CSV → rawzone | SSH COPY → product GCS → rawzone |

## Category

`utilities/55-menu-engineering-vm-land/`
