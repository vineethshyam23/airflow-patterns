# Architecture: absolute multi-channel activity scores

Four layers in one monthly Composer DAG: base extracts, per-channel
rolling activity, thresholded fan-in score, MoM transition flags.

## Diagram

```mermaid
flowchart TB
  subgraph sources [Upstream refined / trusted]
    DEV[(trusted.derived_events)]
    RTRES[(trusted_views.vrt_reservations)]
    ADOBE[(refined.adobe_visit_visitor)]
    SFDC[(CRM call / task facts)]
    ORD[(refined order facts)]
    CB[(refined.customer_base_establishment)]
    TH[(external.as_absolute_kpi_thresholds)]
  end

  subgraph bases [Base extracts WRITE_TRUNCATE]
    B1[as_absolute_base_wb_rt_event]
    B2[as_absolute_base_rt_reservation]
    B3[as_absolute_base_adobe]
    B4[as_absolute_base_adobe_mk]
  end

  subgraph channels [Per-channel WRITE_APPEND as_month]
    WB[wb_login_visitor / wb_event]
    RT[rt_login / rt_event / rt_reservation]
    WL[wl_login]
    MK[mk_login]
    DO[do_login_event / do_order]
    CRM[sfdc_call]
    PORT[portal]
    APP[mobile_app]
  end

  subgraph score [Fan-in]
    AS[as_absolute_activity_score]
    CE[ce_activity_score_transitions]
  end

  DEV --> B1
  RTRES --> B2
  ADOBE --> B3
  ADOBE --> B4
  B3 --> WB
  B3 --> RT
  B3 --> WL
  B3 --> DO
  B1 --> WB
  B1 --> RT
  B2 --> RT
  B4 --> MK
  SFDC --> CRM
  ORD --> DO
  CB --> AS
  TH --> AS
  WB --> AS
  RT --> AS
  WL --> AS
  MK --> AS
  DO --> AS
  CRM --> AS
  PORT --> AS
  APP --> AS
  AS --> CE
```

## Components

**Base extracts**  
Rebuild once per run. The event base drops days with ≥1000 distinct IDs
for the same event label — a blunt anti-spike filter that kept bot-like
menu thrash from inflating scores. Reservation and Adobe bases normalize
product labels and login vs visitor grain.

**Channel tables**  
Each channel projects onto `(salesforce_id, country_code, as_month)` with
rolling 3/6/12-month denominators and country-relative ranks. Partition
field is `as_month`. WRITE_APPEND keeps history for MoM and audits.

**Fan-in score**  
FULL OUTER JOIN across channels, left join threshold table by country
type, compute boolean activity / regular / power flags, then:

`activity_score = activity + regular_user + power_user` (0–3).

Also emits per-product sub-scores and `establishment_active`.

**MoM transitions**  
Compares current vs prior month score and materializes boolean flags
(`has_upscored_*`, `has_downscored_*`, `has_unchanged_*`) for engagement
reporting. WRITE_TRUNCATE — only the latest comparison matrix is needed.

## Why Composer edges instead of one mega-query?

A single SQL job would have been cheaper to author once. It would also
have made partial failure opaque: when Adobe lag killed the run, you
lost reservation and CRM work too. Splitting bases and channels let us
retry the slow path without recomputing everything, and made the fan-in
barrier obvious in the UI when a new channel was added.
