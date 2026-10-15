# Business case: SEO menu content extraction

Pattern 18 already turns SEO / DataForSEO business listings into validated
menu page URLs. Product and ML consumers still need the *content* of those
pages — a stable, re-processable menu object per website — not another
URL discovery pass.

## Problem

Restaurant sites are messy: PDFs and image menus sit next to HTML pages,
TLS is often broken, and a naive parallel crawl races itself when each
worker re-runs `NTILE` while siblings flip statuses. Feeding raw HTML
straight into a generative model also wastes tokens on chrome and scripts.

I needed a control plane that could absorb a multi-country URL ledger,
skip non-HTML early, land artifacts cheaply, and only then spend BQML
budget — with retries that do not lose the last good GCS object.

## Approach

A control landing table is the work queue. Per country: MERGE new URLs as
`pending`, classify (extension fast-path, then HEAD Content-Type), plan a
**stable** NTILE batch map once, then run fixed batch-slot lanes
(fetch → markdown → stage → `AI.GENERATE_TABLE`). Countries come from an
Airflow Variable read at parse time so TaskGroups exist without a BQ call
during DAG import; the end of each run refreshes that Variable for the
next parse.

Page grain stays intentional: one `menu_id` =
`FARM_FINGERPRINT(establishment_id | effective_url)`. Dish explosion is a
separate product problem.

## Tradeoffs

| Choice | Why | Cost |
|--------|-----|------|
| Control table + status machine | Idempotent retries; clear ops view | Extra MERGE surface |
| Pre-planned NTILE work table | Parallel lanes do not reshuffle mid-run | One CREATE OR REPLACE per country per run |
| Fixed batch slots (25) + early return | Graph is parse-stable; unused slots noop | Large TaskGroup when Variable has many countries |
| HTML → markdown before BQML | Smaller prompts; less layout noise | Depends on markdownify quality |
| Content-addressed GCS + sibling delete | Re-fetch replaces prior hash; no orphan pile | List/delete on every success |
| Page-level extract, not dishes | Controllable cost; reusable for later itemization | Downstream still needs a dish pipeline |
| Manual schedule | Stabilize after URL DAG before chaining | Ops triggers until trusted |

## What this is not

Not pattern 18 — URL discovery / Playwright heuristics.
Not pattern 25 — SEO listing NDJSON land into staging.
Not Food Graph ML propagation (#40) or Vertex PipelineJob submit (#45).
Not dish-level menu item extraction.
