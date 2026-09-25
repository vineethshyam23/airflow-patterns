# Business case: Menu Engineering VM Postgres land

## Problem

The menu-engineering product keeps its OLTP database on a GCE VM —
Dockerised Postgres, not Cloud SQL. Platform teams cannot reach it
with Cloud SQL Admin exports. The product team also owns the first
GCS bucket (VM service account already has write). DWH needs a daily
full land into staging before dbt builds trusted models, without
giving Composer SSH+Postgres credentials a second write path that
bypasses product IAM.

A naive "pull everything through Composer" design either:

1. Mounts Composer as a Postgres client across the VPC and fights
   firewall / credential ownership with the product team, or
2. SSHs CSVs straight into the DWH rawzone and forces the VM SA to
   hold DWH bucket IAM — which product will not grant.

Meanwhile the VM fills with leftover CSVs and rotated logs if nobody
cleans up after the upload.

## Decision

Keep the hop that already works and make the dual-bucket handoff
explicit:

1. **SSH + `docker exec` COPY** — Composer triggers export on the
   VM; Postgres never leaves the container network. CSV lands on
   local disk under a dedicated directory.
2. **Product bucket first** — the VM uploads with `gcloud storage cp`
   (parallel composite upload enabled; stale tracker files wiped so
   interrupted composites do not poison the next run).
3. **Composer copies product → DWH rawzone** — `GCSToGCSOperator`
   under a dated prefix. Platform IAM stays on GCS-to-GCS; the VM
   never needs rawzone write.
4. **TRUNCATE load into `me_<table>_tbl`** — schema JSON from the
   raw bucket; no autodetection. dbt owns trusted transforms.
5. **Cleanup after product upload** — journald vacuum, old product
   logs, rotated `/var/log` archives, then delete the CSVs. Disk
   hygiene is part of the DAG, not a separate cron hope.

Fan-out is per table for export / upload / copy / load. Stages are
markers so a single failed table does not hide which hop broke.

## Constraints I cared about

- **No Cloud SQL Admin.** This is not Hydra (#39) or Reservation
  Tool (#54). Retry semantics are SSH / docker / gcloud, not 409
  `operationInProgress`.
- **Dual-bucket is intentional.** Collapsing to one bucket looks
  simpler in a diagram and fails organisationally the first time
  product rotates the VM SA.
- **`max_active_runs=1`.** Overlapping exports fight for the same
  CSV paths and tracker files on the VM.
- **Full dump by design.** Menu-engineering tables are not
  append-only with a clean auto_increment watermark; historization
  stays in dbt, same tradeoff as Hydra.
- **One-time SSH key bootstrap stays out of the daily graph.** Key
  generation + VM metadata injection is a runbook, not a 05:00 task.

## Distinct from nearby Deepideas / enrichment patterns

Patterns 20–22 export *enrichment attributes* (establishments,
category gaps, ingredients) to a partner event bus as Avro. This
pattern lands the *product OLTP* itself. Same product family, two
different engineering jobs.

## What I would not claim

No invented euro savings or headcount. The measurable wins are a
repeatable land without VM-disk growth and a clean IAM boundary
between product bucket and DWH rawzone.
