# Architecture: recommender Vertex weekly mirror

Composer owns schedule, parallelism, and the `copy_jobs` loop.
`insights_share_queries.SHARE_QUERIES` owns the SELECT statements
for the insights destination. Stock `BigQueryInsertJobOperator`
does each full-replace copy — no custom operator.

## Diagram

```mermaid
flowchart TB
  subgraph rex [REX Vertex project]
    M1[("datazone_metro_pl.horeca_shg_potential")]
    M2[("datazone_metro_pl.article_potential")]
  end

  subgraph dwhSrc [DWH project — source tables]
    S1[("refined / trusted / product_spot / …")]
  end

  subgraph composer [Cloud Composer DAG etl_recommender_vertex]
    START[start]
    C1[copy_pol_horeca_shg_potential]
    C2[copy_pol_article_potential]
    CN[copy_* insights_share ×N parallel]
    ENDN[end]
  end

  subgraph registry [copy_jobs registry]
    JOBS["list of table + dataset + SELECT"]
    SHARE[SHARE_QUERIES dict]
  end

  subgraph dwhDest [DWH project — destinations]
    R[("refined.pol_*_potential")]
    I[("insights_share.*")]
  end

  SHARE --> JOBS
  JOBS --> C1
  JOBS --> C2
  JOBS --> CN

  M1 --> C1
  M2 --> C2
  S1 --> CN

  START --> C1
  START --> C2
  START --> CN
  C1 --> ENDN
  C2 --> ENDN
  CN --> ENDN

  C1 --> R
  C2 --> R
  CN --> I
```

## Components

| Piece | Role |
|-------|------|
| `dag_recommender_vertex_weekly_mirror.py` | ENV → project/conn, build `copy_jobs`, fan-out InsertJobs |
| `insights_share_queries.py` | SELECT-only map keyed by destination table id |
| `BigQueryInsertJobOperator` | BATCH + WRITE_TRUNCATE + CREATE_IF_NEEDED |
| Composer SA | Read on REX market zone + DWH sources; write on dest datasets |

## Design notes

- Registry over TaskGroup sprawl: every mirror looks the same in
  the UI (`copy_{table}`), which makes onboarding and failure triage
  predictable.
- Two destination datasets in one DAG on purpose. Splitting by
  destination would duplicate schedule, retries, and env wiring for
  no independent SLA.
- `max_active_runs=1` stops a slow Monday from overlapping next
  week's truncate and producing confusing double-writer races.
