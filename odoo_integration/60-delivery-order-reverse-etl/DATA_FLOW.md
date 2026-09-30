# Data flow: Delivery order reverse ETL

## Daily / on-demand path

1. Operator (or external trigger) queues `etl_delivery_order`.
2. Task builds SQL against `trusted.odoo_stock_picking_delivery_order`,
   ranking by carrier tracking / partition date and excluding ids already
   listed in `ops.delivery_order_exported_ids`.
3. `load_delivery_order_data` materializes the result as a DataFrame.
4. OdooRPC + Postgres connections open; warehouse XMLID resolves
   outbound picking type, customer location, company, and UoM.
5. Batch maps:
   - external ids → existing `stock.picking` res_ids
   - product codes → `product.product` (template fallback)
   - partner UUIDs → delivery `res_partner`
   - machine codes → create-on-miss `machine.code`
6. Per row:
   - **Insert** if no external id: create picking + moves, register
     external id, set `waiting`, attach lots / machine codes, validate
     when fulfilment says shipped/delivered/in-progress.
   - **Update** if external id exists: draft moves, rebuild duplicate
     product lines, write header, return early if already `done`.
7. Postgres connection closes; exception list (availability failures)
   returned to the task logs.

## Grain

One BQ row = one delivery header with a nested `delivery_details`
array (product code, serial, machine code, notes, fulfilment). Odoo
grain is one `stock.picking` plus N `stock.move` / `stock.move.line`.

## Idempotency

External id namespace (`__external_asset_id__` / sanitized equivalent)
is the insert-vs-update switch. Re-running the same unique key updates
the existing picking rather than creating a twin. Control-table
exclusion reduces reprocessing of fully exported carriers; it is not a
substitute for the ir.model.data check.

## Operability notes

- Keep `max_active_runs=1`. Parallel runs race on the same picking.
- Sale-order resolution prefers POS licence split orders, then origin /
  subscription matches — wrong sale link is worse than a null sale_id.
- Do not auto-schedule until the upstream trusted table and control
  table stay fresh; on-demand was the pragmatic SLA during install
  cutovers.
