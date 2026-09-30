# Architecture: Delivery order reverse ETL

Composer runs a single Python task that reads a trusted BigQuery delivery
table, resolves Odoo IDs over Postgres, and creates or updates
`stock.picking` through OdooRPC.

## Diagram

```mermaid
flowchart TB
  subgraph bq [BigQuery]
    TRUSTED[(trusted.odoo_stock_picking_delivery_order)]
    CTRL[(ops.delivery_order_exported_ids)]
  end

  subgraph airflow [Cloud Composer]
    START[EmptyOperator start]
    SYNC[PythonOperator load_delivery_order_data]
    ENDN[EmptyOperator end]
  end

  subgraph sync [OdooDeliveryOrderSync]
    LOOKUP[ir.model.data external-id lookup]
    PROD[product catalog + template fallback]
    PARTNER[delivery partner UUID map]
    BRANCH{picking exists?}
    INSERT[_insert_delivery_order]
    UPDATE[_update_delivery_order]
  end

  subgraph odoo [Odoo ERP]
    PG[(Postgres reads)]
    RPC[OdooRPC writes]
    PICK[stock.picking]
    MOVE[stock.move / move.line]
    LOT[stock.lot + machine.code]
  end

  TRUSTED --> SYNC
  CTRL --> SYNC
  START --> SYNC --> ENDN
  SYNC --> LOOKUP
  LOOKUP --> PG
  PROD --> PG
  PARTNER --> PG
  LOOKUP --> BRANCH
  BRANCH -->|no| INSERT
  BRANCH -->|yes| UPDATE
  INSERT --> RPC
  UPDATE --> RPC
  RPC --> PICK
  RPC --> MOVE
  RPC --> LOT
```

## Components

**DAG (`dag_delivery_order.py`)**  
Env-aware project + Odoo creds from Airflow Variables. Builds the
ranked delivery query (exclude already-exported external ids). One
`PythonOperator` with a one-hour execution timeout. `max_active_runs=1`.
Schedule left `None` — install waves were triggered manually.

**Sync class (`odoo_delivery_order.py`)**  
Dual connection: OdooRPC for create/write/validate, Postgres for
partner / product / lot / external-id lookups. Insert path builds
moves, enables serial tracking when serials exist, registers
`ir.model.data`, sets state to `waiting`, then optionally `done`.
Update path drafts existing moves, rebuilds duplicates, refreshes
header fields, and short-circuits validated pickings to fulfilment /
sale-order link only.

**Connection stub (`odoo_connection.py`)**  
Minimal OdooRPC login. Production should wire pattern 05.

## Failure modes

| Mode | Behaviour |
|------|-----------|
| Odoo / HTTP timeout | Single 60s retry via `retry_on_timeout` (sale-order lookup waits 300s) |
| Missing partner UUID | Log + continue with `partner_id=None` where Odoo allows |
| Missing product code | Skip line; Package codes skipped on multi-line deliveries |
| Stock availability error on lot write | Append carrier ref to exception list; abort that picking |
| Already-validated picking | Header / fulfilment refresh only; no move rebuild |
| Empty BQ result | Early return; DAG succeeds |
