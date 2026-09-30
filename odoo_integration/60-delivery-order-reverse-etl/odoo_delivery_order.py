"""
Odoo stock.picking reverse ETL from BigQuery delivery-order rows.

Reads curated delivery headers + nested line details from the warehouse,
resolves Odoo IDs over Postgres, then creates or updates stock.picking
(with moves, lots, machine codes) through OdooRPC.

Source (read-only): archived `product_installation_odoo.OdooProductInstallation`
delivery-order path. SalesforceProductInstallation land queries are out of
scope for this sample — the DAG assumes a trusted BQ table already exists.
"""

from __future__ import annotations

import datetime
import logging
import time
from collections import Counter
from typing import Any, Dict, List, Optional, Set, Tuple

import pandas as pd
import psycopg2
from google.cloud import bigquery
from psycopg2.extensions import connection as _connection

from odoo_connection import Connection

logger = logging.getLogger(__name__)

TIMEOUT_MARKERS = (
    "Connection timed out",
    "Timeout",
    "timed out",
    "Service Temporarily Unavailable",
    "HTTP",
    "Process timed out",
)

# Odoo ir.model.data module used as the external id namespace for pickings
EXTERNAL_ID_MODULE = "__external_asset_id__"
PRODUCT_CATALOG_MODULE_LIKE = "%product_catalog%"
PRODUCT_TEMPLATE_MODULES = (
    "product_manual",
    "product_catalog_manual",
    "product_catalog",
)
WAREHOUSE_XMLID = "product_stock.product_warehouse_logistics_group"
POS_LICENCE_CODE = "POS_L_Licence"


def serialize_datetime(obj: Any) -> Optional[str]:
    if pd.isna(obj) or obj is pd.NaT:
        return None
    try:
        if isinstance(obj, str):
            obj = pd.to_datetime(obj, errors="coerce")
        if pd.isna(obj):
            return None
        return obj.strftime("%Y-%m-%d %H:%M:%S")
    except Exception as error:
        logger.warning("Error serializing datetime: %s", error)
        return None


def _as_bool(value: Any) -> bool:
    """Parse POS-server-ready flags without eval()."""
    if isinstance(value, bool):
        return value
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return False
    return str(value).strip().lower() in {"true", "1", "yes", "y"}


class OdooDeliveryOrderSync:
    """BigQuery → Odoo stock.picking insert / update for logistics deliveries."""

    def __init__(
        self,
        credentials: Dict[str, str],
        bq_project: str = "dwh_project",
    ) -> None:
        self.credentials = credentials
        self.bq_project = bq_project
        self.odoo_connection: Any = None
        self.postgres_connection: Optional[_connection] = None
        self.cursor: Any = None

    def connect_to_odoo(self) -> Tuple[Any, _connection]:
        conn = Connection()
        try:
            odoo_connection = conn.connect(odoo_creds=self.credentials)
        except Exception as exc:
            if any(marker in str(exc) for marker in TIMEOUT_MARKERS):
                logger.info("Odoo connect timeout; retrying once after 60s")
                time.sleep(60)
                odoo_connection = conn.connect(odoo_creds=self.credentials)
            else:
                raise

        postgres_connection = psycopg2.connect(
            host=self.credentials.get("hostname", ""),
            database=self.credentials.get("database", ""),
            user=self.credentials.get("db_user", ""),
            password=self.credentials.get("db_pwd", ""),
            sslmode="require",
        )
        postgres_connection.autocommit = True
        logger.info("Opened OdooRPC + Postgres connections")
        return odoo_connection, postgres_connection

    def retry_on_timeout(self, func, *args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as exc:
            if any(marker in str(exc) for marker in TIMEOUT_MARKERS):
                logger.info("Timeout in %s; retrying once", func.__name__)
                time.sleep(60)
                return func(*args, **kwargs)
            logger.error("Exception in %s: %s", func.__name__, exc)
            raise

    def set_ir_model(self, asset_id: str, res_id: int, module: str) -> None:
        asset_id = str(asset_id).replace(" ", "")
        self.ir_model_data.create(
            {
                "res_id": res_id,
                "name": asset_id,
                "module": module,
                "model": "stock.picking",
            }
        )

    def _get_id(self, query: str) -> Optional[Any]:
        self.cursor.execute(query)
        result = self.cursor.fetchone()
        return result[0] if result else None

    def _get_ids(self, query: str, multiple: bool = False) -> List[Any]:
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        if not result:
            return []
        ids: List[Any] = []
        for row in result:
            if multiple:
                ids.append([x for x in row])
            else:
                ids.append(row[0])
        return ids

    def _get_partner_id(self, establishment: str) -> Optional[Any]:
        query = f"""
            SELECT id FROM res_partner
            WHERE partner_uuid LIKE '%{establishment}%'
              AND type = 'delivery'
        """
        return self._get_id(query)

    def _get_partner_ids(self, establishments: List[str]) -> Dict[str, Any]:
        if not establishments:
            return {}
        placeholders = ",".join(f"'{est}'" for est in establishments)
        qry = f"""
            SELECT id, partner_uuid FROM res_partner
            WHERE partner_uuid IN ({placeholders})
              AND type = 'delivery'
        """
        self.cursor.execute(qry)
        return {row[1]: row[0] for row in self.cursor.fetchall()}

    def _get_machine_codes(self, machine_codes: Set[str]) -> Dict[str, Any]:
        if not machine_codes:
            return {}
        placeholders = ",".join(f"'{mc}'" for mc in machine_codes)
        qry = f"""
            SELECT id, name FROM machine_code
            WHERE name IN ({placeholders})
        """
        self.cursor.execute(qry)
        res = {row[1]: row[0] for row in self.cursor.fetchall()}
        for mc_name in set(machine_codes) - set(res.keys()):
            res[mc_name] = self.retry_on_timeout(
                self.machine_code_model.create, {"name": mc_name}
            )
        return res

    def _is_lot_available(self, serial_number: str, product_id: int) -> Optional[Any]:
        query = f"""
            SELECT DISTINCT stock_lot.id
            FROM stock_lot
            WHERE stock_lot.name = '{serial_number}'
              AND stock_lot.product_id = {product_id}
        """
        return self._get_id(query)

    def _get_stock_move_line_id(
        self, model_name: str, picking_id: int, filter_condition: str = ""
    ) -> List[Any]:
        query = (
            f"SELECT id FROM {model_name} "
            f"WHERE picking_id = '{picking_id}' {filter_condition}"
        )
        return self._get_ids(query)

    def _get_product_id_map_from_delivery_details(
        self, delivery_details: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        product_codes = list({row["ProductCode"] for row in delivery_details})
        if not product_codes:
            return {}

        placeholders = ",".join(f"'{code}'" for code in product_codes)
        query_primary = f"""
            SELECT imd.name AS product_code, imd.res_id AS product_id
            FROM ir_model_data imd
            JOIN product_product p ON p.id = imd.res_id
            WHERE imd.module LIKE '{PRODUCT_CATALOG_MODULE_LIKE}'
              AND imd.name IN ({placeholders})
              AND imd.model = 'product.product'
              AND p.active = TRUE
        """
        self.cursor.execute(query_primary)
        primary = {row[0]: row[1] for row in self.cursor.fetchall()}

        unresolved = [code for code in product_codes if code not in primary]
        fallback_template_codes = list({code.split(":")[0] for code in unresolved})
        if not fallback_template_codes:
            return primary

        modules = ",".join(f"'{m}'" for m in PRODUCT_TEMPLATE_MODULES)
        fallback_placeholders = ",".join(f"'{code}'" for code in fallback_template_codes)
        query_templates = f"""
            SELECT imd.name AS template_code, imd.res_id AS template_id
            FROM ir_model_data imd
            WHERE imd.module IN ({modules})
              AND imd.name IN ({fallback_placeholders})
              AND imd.model = 'product.template'
        """
        self.cursor.execute(query_templates)
        template_map = {row[0]: row[1] for row in self.cursor.fetchall()}
        if not template_map:
            return primary

        tmpl_ids = ", ".join(str(tid) for tid in template_map.values())
        query_products = f"""
            SELECT id, product_tmpl_id FROM product_product
            WHERE product_tmpl_id IN ({tmpl_ids})
              AND active = TRUE
        """
        self.cursor.execute(query_products)
        tmpl_to_prod = {row[1]: row[0] for row in self.cursor.fetchall()}

        product_id_map: Dict[str, Any] = {}
        for code in product_codes:
            if code in primary:
                product_id_map[code] = primary[code]
            else:
                fallback_code = code.split(":")[0]
                tmpl_id = template_map.get(fallback_code)
                product_id_map[code] = tmpl_to_prod.get(tmpl_id) if tmpl_id else None
        return product_id_map

    def _check_order_status(self, status_list) -> Tuple[str, bool]:
        statuses = [value.lower() for value in list(status_list) if value]
        if any("handed" in status for status in statuses):
            return "To be Handed Over", False
        if any("progress" in status for status in statuses):
            return "In Progress", True
        if any("shipped" in status for status in statuses):
            return "Shipped", True
        if any("delivered" in status for status in statuses):
            return "Delivered", True
        return "Other", False

    def _get_sale_id(
        self, subscription_uuid: str, order_uid: str
    ) -> Optional[Any]:
        query = f"""
            SELECT id FROM (
                SELECT so.split_from_id AS id, 1 AS priority
                FROM sale_order_line sol
                INNER JOIN sale_order so
                    ON sol.order_id = so.id AND so.active = TRUE
                INNER JOIN product_product pp ON sol.product_id = pp.id
                WHERE so.subscription_uuid LIKE '%{subscription_uuid}%'
                  AND pp.product_code = '{POS_LICENCE_CODE}'
                  AND so.split_from_id IS NOT NULL

                UNION ALL

                SELECT so.split_from_id AS id, 2 AS priority
                FROM sale_order_line sol
                INNER JOIN sale_order so
                    ON sol.order_id = so.id AND so.active = TRUE
                INNER JOIN product_product pp ON sol.product_id = pp.id
                WHERE so.origin LIKE '%{order_uid}%'
                  AND pp.product_code = '{POS_LICENCE_CODE}'
                  AND so.split_from_id IS NOT NULL

                UNION ALL

                SELECT so.id, 3 AS priority
                FROM sale_order so
                WHERE so.subscription_uuid LIKE '%{subscription_uuid}%'
                  AND so.active = TRUE

                UNION ALL

                SELECT so.id, 4 AS priority
                FROM sale_order so
                WHERE so.origin LIKE '%{order_uid}%'
                  AND so.active = TRUE
            ) results
            ORDER BY priority
            LIMIT 1
        """
        try:
            return self._get_id(query)
        except Exception as exc:
            if any(marker in str(exc) for marker in TIMEOUT_MARKERS):
                time.sleep(300)
                return self._get_id(query)
            raise

    def _build_stock_move(
        self,
        company_id: int,
        product_id: int,
        location_dest_id: int,
        picking_type_id: Any,
        uom_id: int,
        line: Dict[str, Any],
        row_dict: Dict[str, Any],
        create_date: Optional[str] = None,
    ) -> Tuple[int, int, Dict[str, Any]]:
        move = {
            "company_id": company_id,
            "date": serialize_datetime(row_dict["scheduled_date"])
            or datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "location_dest_id": location_dest_id,
            "location_id": picking_type_id.default_location_src_id.id,
            "name": line["Name"],
            "procure_method": "make_to_stock",
            "product_id": product_id,
            "product_uom": uom_id,
            "product_uom_qty": 1,
            "quantity_done": 1,
            "date_deadline": serialize_datetime(row_dict["date_deadline"]),
        }
        if create_date:
            move["create_date"] = create_date
        return (0, 0, move)

    def _ensure_lot(
        self, serial_number: str, product_id: int, company_id: int
    ) -> Optional[Any]:
        if not serial_number:
            return None
        lot_id = self._is_lot_available(serial_number, product_id)
        if lot_id:
            return lot_id
        created = self.retry_on_timeout(
            self.stock_lot.create,
            {
                "name": serial_number,
                "product_id": product_id,
                "company_id": company_id,
            },
        )
        return [created]

    def _resolve_partner(
        self,
        row_dict: Dict[str, Any],
        establishment_id_map: Dict[str, Any],
    ) -> Optional[Any]:
        partner_uuid = row_dict.get("partner_uuid")
        partner_id = establishment_id_map.get(partner_uuid) if partner_uuid else None
        if not partner_id and partner_uuid:
            logger.info("No establishment in batch map for %s", partner_uuid)
            partner_id = self._get_partner_id(establishment=partner_uuid)
        return partner_id

    def _enable_serial_tracking(self, product_ids: Set[int]) -> None:
        if not product_ids:
            return
        try:
            self.retry_on_timeout(
                self.odoo_connection.execute,
                "product.product",
                "write",
                list(product_ids),
                {"tracking": "serial"},
            )
        except Exception:
            # Serial tracking is best-effort; picking still proceeds
            logger.exception("Could not enable serial tracking")

    def _write_move_line_lot(
        self,
        move_line_id: int,
        lot_id: Any,
        machine_code_id: Any,
        carrier_tracking_ref: Optional[str],
        exception_list: List[str],
    ) -> bool:
        """Write lot / machine code onto a move line. Returns False if abort."""
        try:
            self.retry_on_timeout(
                self.odoo_connection.execute,
                "stock.move.line",
                "write",
                move_line_id,
                {
                    "lot_id": lot_id,
                    "machine_code_id": machine_code_id,
                },
            )
            return True
        except Exception as exc:
            if "check the availability" in str(exc):
                exception_list.append(carrier_tracking_ref or "")
                return False
            logger.info("Move-line lot write failed: %s", exc)
            return True

    def _insert_delivery_order(
        self,
        row_dict: Dict[str, Any],
        carrier_tracking_ref: Optional[str],
        ir_model_name: str,
        module: str,
        picking_type_id: Any,
        location_dest_id: int,
        company_id: int,
        uom_id: int,
        product_id_map: Dict[str, Any],
        establishment_id_map: Dict[str, Any],
        machine_codes: Dict[str, Any],
        exception_list: Optional[List[str]] = None,
    ) -> None:
        exception_list = exception_list if exception_list is not None else []
        details = row_dict["delivery_details"]
        stock_move_list: List[Tuple[int, int, Dict[str, Any]]] = []

        pos_server_ready = any(
            _as_bool(line.get("pos_server_ready")) for line in details
        )
        fulfilment_status, should_validate = self._check_order_status(
            {line.get("fulfilment_status") for line in details}
        )

        sale_id = self._get_sale_id(
            subscription_uuid=row_dict.get("subscription_uuid"),
            order_uid=row_dict.get("order_uid"),
        )

        notes: List[str] = []
        update_product_dict: Dict[str, int] = {}
        product_ids_to_update: Set[int] = set()

        for line in details:
            update_product_dict[line["ProductCode"]] = 0
            notes.append(line["note"] if line.get("note") else "")
            product_id = product_id_map.get(line["ProductCode"])
            if not product_id or (
                "Package" in line["ProductCode"] and len(details) > 1
            ):
                continue

            if line.get("SerialNumber"):
                product_ids_to_update.add(product_id)
                self._ensure_lot(line["SerialNumber"], product_id, company_id)

            create_date = serialize_datetime(line.get("create_date")) or (
                datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            )
            stock_move_list.append(
                self._build_stock_move(
                    company_id,
                    product_id,
                    location_dest_id,
                    picking_type_id,
                    uom_id,
                    line,
                    row_dict,
                    create_date=create_date,
                )
            )

        self._enable_serial_tracking(product_ids_to_update)
        partner_id = self._resolve_partner(row_dict, establishment_id_map)

        create_dates = [
            datetime.datetime.strptime(move[2]["create_date"], "%Y-%m-%d %H:%M:%S")
            for move in stock_move_list
            if move[2].get("create_date")
        ]
        header_create_date = (
            serialize_datetime(min(create_dates)) if create_dates else False
        )

        insert_data = {
            "remote_rack_date": serialize_datetime(row_dict.get("remote_rack_date")),
            "sale_id": sale_id if sale_id else False,
            "note": ",".join(notes) if notes else "",
            "fulfilment_status": fulfilment_status,
            "scheduled_date": serialize_datetime(row_dict.get("scheduled_date"))
            or datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "date_deadline": serialize_datetime(row_dict.get("date_deadline")),
            "shipment_date": serialize_datetime(row_dict.get("shipment_date")),
            "delivery_date": serialize_datetime(row_dict.get("delivery_date")),
            "establishment_id": partner_id,
            "carrier_tracking_ref": carrier_tracking_ref,
            "origin": details[0]["origin"] if details else None,
            "handover_date": row_dict.get("handover_date") or None,
            "partner_id": partner_id,
            "picking_type_id": picking_type_id.id,
            "location_dest_id": location_dest_id,
            "location_id": picking_type_id.default_location_src_id.id,
            "move_type": "one",
            "move_ids": stock_move_list,
            "pos_server_ready": pos_server_ready,
            "message_ids": [
                (
                    0,
                    0,
                    {
                        "model": "stock.picking",
                        "body": f"create_date: {header_create_date}",
                        "message_type": "comment",
                        "email_from": False,
                        "reply_to": False,
                    },
                )
            ],
        }

        res_id = self.retry_on_timeout(self.stock_picking_model.create, insert_data)
        logger.info("Created stock.picking id=%s", res_id)
        self.set_ir_model(asset_id=ir_model_name, res_id=res_id, module=module)

        picking_id = self.retry_on_timeout(self.stock_picking_model.browse, res_id)
        self.retry_on_timeout(picking_id.write, {"state": "waiting"})

        for line in details:
            product_id = product_id_map.get(line["ProductCode"])
            if not product_id or (
                "Package" in line["ProductCode"] and len(details) > 1
            ):
                continue

            machine_code_id = False
            if line.get("MachineCode"):
                machine_code_id = machine_codes.get(line["MachineCode"])

            lot_id = self._ensure_lot(
                line.get("SerialNumber"), product_id, company_id
            )
            move_line = sorted(
                self._get_stock_move_line_id(
                    model_name="stock_move_line",
                    picking_id=picking_id.id,
                    filter_condition=f" AND product_id = {product_id}",
                )
            )
            if move_line:
                idx = update_product_dict[line["ProductCode"]]
                ok = self._write_move_line_lot(
                    move_line[idx],
                    lot_id,
                    machine_code_id,
                    carrier_tracking_ref,
                    exception_list,
                )
                if not ok:
                    return
            update_product_dict[line["ProductCode"]] += 1

        if should_validate and picking_id.move_ids:
            self.retry_on_timeout(
                picking_id.move_ids_without_package.write, {"state": "done"}
            )
            self.retry_on_timeout(picking_id.write, {"state": "done"})

    def _update_delivery_order(
        self,
        res_id: int,
        row_dict: Dict[str, Any],
        carrier_tracking_ref: Optional[str],
        picking_type_id: Any,
        location_dest_id: int,
        company_id: int,
        uom_id: int,
        product_id_map: Dict[str, Any],
        establishment_id_map: Dict[str, Any],
        machine_codes: Dict[str, Any],
        exception_list: Optional[List[str]] = None,
    ) -> None:
        exception_list = exception_list if exception_list is not None else []
        details = row_dict["delivery_details"]

        self.retry_on_timeout(self.stock_picking_model.write, [res_id], {"state": "draft"})

        pos_server_ready = any(
            _as_bool(line.get("pos_server_ready")) for line in details
        )
        fulfilment_status, should_validate = self._check_order_status(
            {line.get("fulfilment_status") for line in details}
        )
        partner_id = self._resolve_partner(row_dict, establishment_id_map)

        try:
            picking_id = self.retry_on_timeout(self.stock_picking_model.browse, res_id)
            if partner_id and not getattr(picking_id, "establishment_id", None):
                picking_id.write(
                    {"establishment_id": partner_id, "partner_id": partner_id}
                )
            picking_id.move_line_ids_without_package.write({"state": "draft"})
            picking_id.move_ids_without_package.write({"state": "draft"})
            picking_state = picking_id.state
            stock_picking_id = picking_id.id
        except Exception as exc:
            logger.info("Browse/draft failed for picking %s: %s", res_id, exc)
            return

        sale_id = self._get_sale_id(
            subscription_uuid=row_dict.get("subscription_uuid"),
            order_uid=row_dict.get("order_uid"),
        )

        # Already-validated pickings: refresh fulfilment / sale link only
        if picking_state == "done":
            update_dict: Dict[str, Any] = {"fulfilment_status": fulfilment_status}
            try:
                if sale_id:
                    picking_id.write({"sale_id": sale_id})
                picking_id.write(update_dict)
            except Exception as exc:
                if any(marker in str(exc) for marker in TIMEOUT_MARKERS):
                    time.sleep(60)
                    if sale_id:
                        picking_id.write({"sale_id": sale_id})
                    picking_id.write(update_dict)

        # Duplicate product codes on a picking: wipe moves so we can rebuild
        product_code_counts = Counter(line["ProductCode"] for line in details)
        for product_code, count in product_code_counts.items():
            if count <= 1:
                continue
            product_id = product_id_map.get(product_code)
            if not product_id:
                logger.info("Product not found for %s", product_code)
                continue
            move_lines = [
                ml
                for ml in self._get_stock_move_line_id(
                    "stock_move_line", picking_id.id
                )
                if self.stock_move_line.browse(ml).product_id.id == product_id
            ]
            moves = [
                mv
                for mv in self._get_stock_move_line_id("stock_move", picking_id.id)
                if self.stock_move.browse(mv).product_id.id == product_id
            ]
            for line_id in move_lines:
                rec = self.stock_move_line.browse(line_id)
                rec.write({"reserved_uom_qty": 0.0, "qty_done": 0.0})
                rec.unlink()
            for move_id in moves:
                rec = self.stock_move.browse(move_id)
                rec.write({"quantity_done": 0, "product_uom_qty": 0})
                rec.unlink()

        notes: List[str] = []
        stock_move_list: List[Tuple[int, int, Dict[str, Any]]] = []
        update_product_dict: Dict[str, int] = {}
        product_ids_to_update: Set[int] = set()

        for line in details:
            update_product_dict[line["ProductCode"]] = 0
            notes.append(line["note"] if line.get("note") else "")
            product_id = product_id_map.get(line["ProductCode"])
            if not product_id or (
                "Package" in line["ProductCode"] and len(details) > 1
            ):
                continue
            if line.get("SerialNumber"):
                product_ids_to_update.add(product_id)

            if picking_state == "done":
                logger.info(
                    "Skipping move rebuild for validated picking %s", picking_id.name
                )

            move_line_ids = self._get_stock_move_line_id(
                "stock_move_line", stock_picking_id
            )
            products = [
                self.stock_move_line.browse(ml).product_id.id for ml in move_line_ids
            ]
            if product_id not in products:
                stock_move_list.append(
                    self._build_stock_move(
                        company_id,
                        product_id,
                        location_dest_id,
                        picking_type_id,
                        uom_id,
                        line,
                        row_dict,
                    )
                )
            elif row_dict.get("date_deadline"):
                matching = [
                    ml
                    for ml in move_line_ids
                    if self.stock_move_line.browse(ml).product_id.id == product_id
                ]
                if matching:
                    self.retry_on_timeout(
                        self.stock_move_line.browse(matching).move_id.write,
                        {
                            "date_deadline": serialize_datetime(
                                row_dict["date_deadline"]
                            )
                        },
                    )

        self._enable_serial_tracking(product_ids_to_update)

        update_data: Dict[str, Any] = {
            "remote_rack_date": serialize_datetime(row_dict.get("remote_rack_date")),
            "sale_id": sale_id if sale_id else False,
            "note": ",".join(notes) if notes else "",
            "scheduled_date": serialize_datetime(row_dict.get("scheduled_date"))
            or datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "date_deadline": serialize_datetime(row_dict.get("date_deadline")),
            "shipment_date": serialize_datetime(row_dict.get("shipment_date")),
            "delivery_date": serialize_datetime(row_dict.get("delivery_date")),
            "establishment_id": partner_id,
            "carrier_tracking_ref": carrier_tracking_ref,
            "origin": details[0]["origin"] if details else None,
            "handover_date": serialize_datetime(row_dict.get("handover_date")),
            "partner_id": partner_id,
            "move_ids": stock_move_list,
            "pos_server_ready": pos_server_ready,
        }
        if picking_state == "done" or not stock_move_list:
            update_data.pop("move_ids", None)
            if picking_state == "done":
                update_data.pop("scheduled_date", None)

        self.retry_on_timeout(picking_id.write, update_data)

        if picking_state == "draft":
            self.retry_on_timeout(picking_id.write, {"state": "waiting"})

        if picking_state == "done":
            return

        for line in details:
            product_id = product_id_map.get(line["ProductCode"])
            if not product_id or (
                "Package" in line["ProductCode"] and len(details) > 1
            ):
                continue

            machine_code_id = False
            if line.get("MachineCode"):
                machine_code_id = machine_codes.get(line["MachineCode"])

            lot_id = self._ensure_lot(
                line.get("SerialNumber"), product_id, company_id
            )
            try:
                move_line = sorted(
                    self._get_stock_move_line_id(
                        "stock_move_line",
                        stock_picking_id,
                        filter_condition=f" AND product_id = {product_id}",
                    )
                )
            except ValueError:
                move_line = []

            if move_line:
                idx = update_product_dict[line["ProductCode"]]
                ok = self._write_move_line_lot(
                    move_line[idx],
                    lot_id,
                    machine_code_id,
                    carrier_tracking_ref,
                    exception_list,
                )
                if not ok:
                    return
            update_product_dict[line["ProductCode"]] += 1

        if should_validate and picking_id.move_ids and picking_state != "done":
            self.retry_on_timeout(
                picking_id.write,
                {"fulfilment_status": fulfilment_status, "state": "done"},
            )
            move_ids = self._get_stock_move_line_id("stock_move", stock_picking_id)
            line_ids = self._get_stock_move_line_id(
                "stock_move_line", stock_picking_id
            )
            if move_ids:
                self.retry_on_timeout(
                    self.stock_move.write, move_ids, {"state": "done"}
                )
            if line_ids:
                self.retry_on_timeout(
                    self.stock_move_line.write, line_ids, {"state": "done"}
                )

    def load_delivery_order_data(
        self,
        query: str,
        batch_start: int = 0,
        batch_size: int = 100,
        orders: Optional[List[Any]] = None,
    ) -> List[str]:
        del batch_start, batch_size, orders  # kept for call-site parity
        logger.info("Starting delivery-order reverse ETL")

        bigquery_client = bigquery.Client(project=self.bq_project)
        delivery_orders = bigquery_client.query(query).result().to_dataframe()
        logger.info("Found %s delivery orders", len(delivery_orders))

        def initialize_odoo_objects() -> Dict[str, Any]:
            self.odoo_connection, self.postgres_connection = self.connect_to_odoo()
            env = self.odoo_connection.env
            self.stock_picking_model = env["stock.picking"]
            self.ir_model_data = env["ir.model.data"]
            self.stock_move = env["stock.move"]
            self.stock_move_line = env["stock.move.line"]
            self.stock_lot = env["stock.lot"]
            self.machine_code_model = env["machine.code"]
            warehouse = env.ref(WAREHOUSE_XMLID)
            return {
                "picking_type_id": warehouse.out_type_id,
                "location_dest_id": env.ref("stock.stock_location_customers").id,
                "company_id": env.ref("base.main_company").id,
                "uom_id": env.ref("uom.product_uom_unit").id,
            }

        odoo_refs = self.retry_on_timeout(initialize_odoo_objects)
        picking_type_id = odoo_refs["picking_type_id"]
        location_dest_id = odoo_refs["location_dest_id"]
        company_id = odoo_refs["company_id"]
        uom_id = odoo_refs["uom_id"]

        if delivery_orders.empty:
            logger.info("No delivery orders to process")
            return []

        exception_list: List[str] = []
        assert self.postgres_connection is not None
        self.cursor = self.postgres_connection.cursor()

        keys: List[str] = []
        name_map: Dict[str, str] = {}
        for row in delivery_orders.to_dict(orient="records"):
            unique_key = row.get("unique_key") or ""
            if not unique_key:
                continue
            name_part = (
                unique_key.split("=")[1] if "=" in unique_key else unique_key
            )
            ir_model_name = name_part.replace(" ", "")
            keys.append(ir_model_name)
            name_map[unique_key] = ir_model_name

        res_id_map: Dict[str, int] = {}
        if keys:
            format_strings = ",".join(["%s"] * len(keys))
            lookup = f"""
                SELECT name, res_id FROM ir_model_data
                WHERE module = '{EXTERNAL_ID_MODULE}'
                  AND model = 'stock.picking'
                  AND name IN ({format_strings})
            """
            self.cursor.execute(lookup, tuple(keys))
            res_id_map = {row[0]: row[1] for row in self.cursor.fetchall()}

        delivery_details = [
            detail
            for row in delivery_orders["delivery_details"]
            for detail in row
        ]
        product_id_map = self._get_product_id_map_from_delivery_details(
            delivery_details
        )
        establishments = (
            delivery_orders["partner_uuid"].dropna().unique().tolist()
        )
        establishment_id_map = self._get_partner_ids(establishments)
        machine_code_names = {
            detail["MachineCode"]
            for detail in delivery_details
            if detail.get("MachineCode")
        }
        machine_code_map = self._get_machine_codes(machine_code_names)

        for row in delivery_orders.to_dict(orient="records"):
            rnk = row.get("rnk")
            carrier_tracking_ref = row.get("carrier_tracking_ref") or None
            unique_key = row.get("unique_key") or ""
            ir_model_name = name_map.get(unique_key, "").replace(" ", "")
            res_id = res_id_map.get(ir_model_name)
            row_dict = dict(row)
            row_dict["res_id"] = res_id

            try:
                if not res_id:
                    self._insert_delivery_order(
                        row_dict=row_dict,
                        carrier_tracking_ref=carrier_tracking_ref,
                        ir_model_name=ir_model_name,
                        module=EXTERNAL_ID_MODULE,
                        picking_type_id=picking_type_id,
                        location_dest_id=location_dest_id,
                        company_id=company_id,
                        uom_id=uom_id,
                        product_id_map=product_id_map,
                        establishment_id_map=establishment_id_map,
                        machine_codes=machine_code_map,
                        exception_list=exception_list,
                    )
                    logger.info("Inserted rnk=%s unique_key=%s", rnk, ir_model_name)
                else:
                    self._update_delivery_order(
                        res_id=res_id,
                        row_dict=row_dict,
                        carrier_tracking_ref=carrier_tracking_ref,
                        picking_type_id=picking_type_id,
                        location_dest_id=location_dest_id,
                        company_id=company_id,
                        uom_id=uom_id,
                        product_id_map=product_id_map,
                        establishment_id_map=establishment_id_map,
                        machine_codes=machine_code_map,
                        exception_list=exception_list,
                    )
                    logger.info("Updated rnk=%s unique_key=%s", rnk, ir_model_name)
            except Exception as exc:
                if any(marker in str(exc) for marker in TIMEOUT_MARKERS):
                    time.sleep(60)
                    if not res_id:
                        self._insert_delivery_order(
                            row_dict=row_dict,
                            carrier_tracking_ref=carrier_tracking_ref,
                            ir_model_name=ir_model_name,
                            module=EXTERNAL_ID_MODULE,
                            picking_type_id=picking_type_id,
                            location_dest_id=location_dest_id,
                            company_id=company_id,
                            uom_id=uom_id,
                            product_id_map=product_id_map,
                            establishment_id_map=establishment_id_map,
                            machine_codes=machine_code_map,
                            exception_list=exception_list,
                        )
                    else:
                        self._update_delivery_order(
                            res_id=res_id,
                            row_dict=row_dict,
                            carrier_tracking_ref=carrier_tracking_ref,
                            picking_type_id=picking_type_id,
                            location_dest_id=location_dest_id,
                            company_id=company_id,
                            uom_id=uom_id,
                            product_id_map=product_id_map,
                            establishment_id_map=establishment_id_map,
                            machine_codes=machine_code_map,
                            exception_list=exception_list,
                        )
                else:
                    raise

        if self.postgres_connection is not None:
            self.postgres_connection.autocommit = False
            self.cursor.close()
            self.postgres_connection.close()
            logger.info("Closed Postgres connection")

        return exception_list
