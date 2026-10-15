# Pattern 63: Composer dev DAG snapshot

Sanitized copy of a small Composer 3 environment (Airflow 2.10.5) that
scheduled Postgres and BigQuery jobs. The environment is being removed.
This folder is the keep-forever copy of the DAG bucket, plus a script
that repeats the export.

Passwords, API keys, and tokens in `snapshot/` are `REDACTED`. Project
ids, bucket names, and email addresses are generalized. Connection and
variable dumps are not in this repo.

## Files

| File | Role |
|------|------|
| `export_composer_dags.py` | Copy a `gs://.../dags` prefix to a backup bucket and write a path manifest |
| `snapshot/` | DAG, SQL, and helper modules from the environment bucket |

## Quick start

```bash
python -c "import ast; ast.parse(open('export_composer_dags.py').read())"
python export_composer_dags.py --source gs://composer-bucket/dags --dest gs://backup-bucket/composer-snapshot
```

Run the export before deleting a Composer environment. Deletion removes
the managed DAG bucket.
