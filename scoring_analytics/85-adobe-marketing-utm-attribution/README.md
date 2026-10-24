# Pattern 85: Adobe marketing UTM attribution

Daily Composer job that appends today's website visits from the
refined Adobe Analytics datafeed into a marketing attribution table
with conversion-funnel flags, referral-channel pivots, and UTM eVars.

This is the *attribution* projection on top of hit land. It is not
the Data Feed unpack (patterns 49 / 51) and not the Hydra site-
traffic CSV publish (pattern 77).

Source (read-only):
- `dags/adobe_marketing_utm.py`

## Files

| File | Role |
|------|------|
| `adobe_marketing_utm_query.py` | INSERT…SELECT builder (funnel / referral / UTM) |
| `dag_adobe_marketing_utm.py` | Single-task daily DAG |
| `BUSINESS_CASE.md` | Why a thin append instead of folding into rawfeed |
| `ARCHITECTURE.md` | Components + Mermaid diagram |
| `DATA_FLOW.md` | Schedule, rules, idempotency debt |

## Quick start

```bash
python -c "import ast; ast.parse(open('adobe_marketing_utm_query.py').read())"
python -c "import ast; ast.parse(open('dag_adobe_marketing_utm.py').read())"
```

Needs `bigquery_default`, Variables `dwh_project_id` /
`refined_dataset` / optional `adobe_utm_product_type`, and a
populated `refined.adobe_datafeed` for the visit date. This folder
is a sanitized reference, not a deploy package.

## Sanitization notes

- GCP project `hd-dwh-stream-1` → Variable `dwh_project_id` (`dwh_project`)
- Dataset `dwh_refined` → `refined`
- Product filter brand code → Variable `adobe_utm_product_type` (`PLATFORM`)
- Owner / emails → `data-platform` / `dataops@example.com`
- Brochure docstring trimmed; production debt called out in docs
- SQL extracted to `adobe_marketing_utm_query.py` for reuse / tests

## Distinct from nearby patterns

| | 49 / 51 (Data Feed land) | 77 (CSV publish) | 85 (this) |
|---|---|---|---|
| Direction | Vendor → warehouse | Warehouse → consumer files | Hit table → attribution table |
| Grain | Hit / lookup unpack | Daily metric CSVs | Visit/hit attribution flags |
| Hard part | tar/tsv + lookups | Overwrite + retention | Funnel / referral SQL + schedule debt |

## Category

`scoring_analytics/85-adobe-marketing-utm-attribution/`
