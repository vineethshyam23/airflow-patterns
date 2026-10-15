# Pattern 48: Offer Tool weekday-aware multi-project zone

Composer DAG that publishes warehouse refined / trusted tables into
per-stage Offer Tool GCP projects. Wednesday widens fan-out to
acc + stg + prod; other days refresh prod only. DEV short-circuits to
Mondays for the benchmarking branch.

Distinct from pattern 27 (Cloud SQL SCD ingest of the Offer Tool OLTP)
and pattern 46 (Food Graph refined zone inside the DWH). This pattern
owns the *product zone publish* contract.

Source (read-only):
- `dags/etl_customized_offering_zone.py`
- `dags/horeca_digital/customized_offering_queries.py`

## Files

| File | Role |
|------|------|
| `dag_customized_offerings_zone.py` | Stage resolution, country loops, validation chain |
| `zone_queries.py` | Nest gaps, assortment, FBO scores, article rec builders |
| `BUSINESS_CASE.md` | Why weekday fan-out + product-owned projects |
| `ARCHITECTURE.md` | Components + Mermaid diagram |
| `DATA_FLOW.md` | Paths A–E, failure modes |

## Quick start

```bash
python -c "import ast; ast.parse(open('zone_queries.py').read())"
python -c "import ast; ast.parse(open('dag_customized_offerings_zone.py').read())"
python -c "
import zone_queries as q
assert 'wholesale' in q.exclude_deleted_statement(field='wholesale_id', iso_code='DE')
assert 'Platform Premium' in q.fbo_scores_export('NL')
print('queries ok')
"
# Full DAG import needs apache-airflow + google providers.
```

Needs BigQuery IAM on the DWH project (read) and each Offer Tool
project (write to `offer_tool_zone`), plus a GCP connection. This
folder is a sanitized reference, not a deploy package.

## Sanitization notes

- GCP projects `hd-dwh-stream-1*` → `dwh_project` / env `PROJECT`
- Offer Tool projects `customized-offering-*-#####` → `offer-tool-{dev,acc,stg,prod}`
- Dataset `cocs_bevelop` → `offer_tool_zone`
- `metro_*` / MCC naming → `wholesale_*`
- Platform product bundle labels anonymized (`DISH *` → `Platform *`)
- Emails / Slack channel / webhook conn → `dataops@example.com` + log stub
- Package import `horeca_digital.customized_offering_queries` → local
  `zone_queries`
- Sibling on-demand establishments/ES DAG omitted (optional follow-up)
- Author names removed; `max_active_runs=1` added

## Distinct from patterns 27 / 46

| | 27 | 46 | 48 (this) |
|---|----|----|-----------|
| Question | Land Offer Tool OLTP with history | Build refined Food Graph in DWH | Publish DWH → product projects |
| Destination | DWH trusted SCD | `refined_foodgraph` | `offer-tool-*`.`offer_tool_zone` |
| Cadence lever | Daily sequential dumps | Daily country barrier | Weekday stage fan-out |
| Grain | 15 OLTP tables × SCD2 | Country analytics chains | Stage × country snapshots |

## Category

`sql_patterns/48-customized-offerings-zone/`
