# Business case: Delivery order reverse ETL

Field logistics and POS install teams work in Odoo. The warehouse already
holds a curated view of delivery orders (carrier tracking, serials,
fulfilment status, partner UUIDs) built from upstream CRM / install
feeds. Ops needed those rows to become real `stock.picking` records —
not another warehouse report.

I treated this as reverse ETL, not another export. BigQuery owns the
join and ranking; Odoo owns inventory state, lots, and validation. That
split kept the Composer DAG thin (one Python task) while concentrating
the hard bits — external-id insert vs update, product catalog resolution,
serial tracking, draft→waiting→done — in one sync class.

Why not push straight from Salesforce into Odoo? The warehouse table is
the contract. Ranking, exclusion of already-exported ids, and nested
`delivery_details` arrays are cheaper to reason about in SQL than in a
CRM webhook. When a picking already exists (`ir.model.data` lookup), we
update; when it does not, we create and register the external id. That
idempotency mattered more than schedule polish — the production DAG
ran on demand (`schedule_interval=None`) while install waves were
unstable.

Tradeoffs I accepted:

- Dual connection (OdooRPC write + Postgres read) costs ops complexity
  but avoids slow ORM reads for partner / product / lot lookups.
- Validating pickings from fulfilment status is aggressive; stock
  availability errors are collected rather than failing the whole batch.
- Package lines are skipped when a multi-line delivery already has
  concrete product codes — otherwise Odoo invents empty moves.

Distinct from inbound Odoo patterns 06–15 and 42 (CRM lead push): this
path writes logistics documents outbound from the DWH into ERP stock.
