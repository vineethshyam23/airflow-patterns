# Business case: Overnight multi-country POS land

POS analytics only works if three things land together before the
business day: vendor master (who owns which machine), daily tickets
(what was sold), and tenant–debtor mapping (how a ticket ties back to a
customer). Split those across ad-hoc scripts and you spend the morning
explaining gaps in dashboards.

I kept this as one overnight Composer DAG rather than five country DAGs
or a pure dbt Cloud schedule for three practical reasons.

**One SLA, one failure surface.** Sales and finance treat "POS is ready
by 09:00" as a single promise. Fan-out per country still runs in
parallel (`max_active_tasks=15`), but fan-in before matching / POS dbt
means we do not publish half a continent.

**Backfill without a second codebase.** Gap fills and initial country
rollouts used to fork copies of the loader. The date-range toggle
(Variables, or a temporary module override) reuses the same load →
country dbt → move path for N days in one run. Cost is sequential days
per country; benefit is no divergent "backfill DAG" to forget about.

**Afternoon stays thin.** Pattern 43 re-lands debtor/location at 13:00
because customer master drifts during the day. Overnight still owns
machines, articles, tickets, and mapping. Separating those graphs kept
midday latency low without teaching the afternoon job how to replay a
week of tickets.

Tradeoff I accepted: master file pick is still "first list_blobs match
for today's token," which is weaker than afternoon's newest-by-mtime
logic. Unifying that is a small follow-up; inventing a second overnight
DAG is not.

What this pattern is not claiming: dollar savings, team size, or vendor
SLAs. It is the production shape that kept five markets on one schedule
without turning Composer into a shell around five unrelated jobs.
