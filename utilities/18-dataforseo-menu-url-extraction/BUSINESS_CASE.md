# Business case: SEO menu URL extraction

Menu-gap and recommendation products need a *navigable menu page*, not
just a homepage. Upstream SEO business listings often have a website
and sometimes a vendor-supplied `menu_url` — but coverage is incomplete
and many "menu" fields are social links, PDFs, or dead pages.

I did not put an LLM in the fetch loop. Classification cost and latency
at multi-country scale dominate, and keyword + schema.org heuristics
already catch the languages we care about (DE/ES/FR/IT + EN). The hard
part is operability: retries, bot blocks, JS-only SPAs, and not
re-crawling completed rows.

## What this unlocked

- Deterministic discovery from HTML (anchors, JSON-LD HoReCa types,
  Next/Nuxt hydration, data-* router attrs) with one normalize path
- Prefer vendor `menu_url` when HEAD/GET says it is alive — skip crawl
- Per-country NTILE partitions so Composer workers stay memory-safe
- Mini-batch BQ flushes (250) so a 72h DAG run does not lose hours of
  progress on a single worker kill
- Optional Cloud Run Playwright only when HTTP hits 403/429/timeout or
  a configured JS-heavy domain — not for every URL

## Constraints

- Country TaskGroups are built at *parse* time from an Airflow
  Variable. First run (or empty Variable) skips Phase 2 until
  `update_variable_null_menuurls` populates sizes. That is awkward but
  keeps parse-time BQ calls out of the DAG file.
- `max_active_tasks=5` caps cross-country parallelism. Raise carefully —
  each batch holds an HTTP pool + thread workers.
- Own-domain 403 is treated as "URL exists" during validation. Third-
  party hosts (Instagram, Facebook, …) are skipped — they are never
  usable menu pages for us.
- Heuristics miss menus with no keyword in path/text and no schema.org
  Menu type. Accept that miss rate or expand keyword lists; do not
  invent recall numbers.

## What this is not

Not ranked menu-gap opportunity scores (patterns 12/14). Not the SEO
GCS → BQ ingest DAG. Not the Playwright service itself — only the
caller that posts `{url}` with an identity token.
