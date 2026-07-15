"""
Domain class for loading account.move (invoices) into Odoo.

Handles complex invoice creation with:
- Partner resolution via external IDs
- Product/tax lookups
- Invoice line creation
- Error tracking to GCS
"""

import logging
from typing import List, Dict, Optional, Any
from google.cloud import bigquery, storage
from odoo_utils import Connection

logger = logging.getLogger(__name__)


class Odoo:
    """
    Handles Odoo account.move (invoice) operations.
    
    Uses dual connection strategy:
    - OdooRPC for writes (create, update)
    - PostgreSQL for reads (faster lookups)
    """
    
    def __init__(self, credentials: Dict[str, str] = None):
        """
        Initialize Odoo connections.
        
        Args:
            credentials: Odoo connection credentials
        """
        self.timeout_list = [
            "Connection timed out",
            "Timeout",
            "timed out",
            "Service Temporarily Unavailable"
        ]
        
        logger.info("Initializing Odoo connection")
        conn = Connection()
        self.odoo = conn.connect(credentials)
        self.pg_connection = conn.connect_pg(credentials)
        self.cursor = self.pg_connection.cursor()
        logger.info("Successfully initialized connections")
    
    def fetchone(self, query: str) -> Optional[Any]:
        """Execute PostgreSQL query and fetch one result."""
        self.cursor.execute(query)
        result = self.cursor.fetchone()
        return result[0] if result else None
    
    def fetchall(self, query: str) -> List[tuple]:
        """Execute PostgreSQL query and fetch all results."""
        self.cursor.execute(query)
        return self.cursor.fetchall()
    
    def _resolve_partner_id(self, row: Dict) -> Optional[int]:
        """
        Resolve partner ID from UUID using external identifiers.
        
        Tries multiple lookup strategies:
        1. Direct UUID match in dish_partner_uuid field
        2. External ID in ir_model_data table
        3. Fallback to dummy establishment creation
        
        Args:
            row: Dictionary containing partner_uuid
        
        Returns:
            Partner ID or None
        """
        # Try direct lookup
        query = f"""
        SELECT id 
        FROM res_partner 
        WHERE dish_partner_uuid = '{row['partner_uuid']}'
          AND active IN (true, false)
        LIMIT 1
        """
        partner_id = self.fetchone(query)
        
        if partner_id:
            return partner_id
        
        # Try external ID lookup
        query = f"""
        SELECT res_id 
        FROM ir_model_data 
        WHERE module = 'salesforce'
          AND model = 'res.partner'
          AND name = '{row['partner_uuid']}'
        """
        partner_id = self.fetchone(query)
        
        if partner_id:
            return partner_id
        
        logger.warning(f"Partner {row['partner_uuid']} not found")
        return None
    
    def _resolve_product_id(self, product_code: str) -> Optional[int]:
        """
        Resolve product ID from product code.
        
        Args:
            product_code: Product external reference (e.g., "POS_L_Package:1:1")
        
        Returns:
            Product ID or None
        """
        query = f"""
        SELECT res_id 
        FROM ir_model_data 
        WHERE module = 'dish_product_catalog'
          AND name = '{product_code}'
        """
        return self.fetchone(query)
    
    def _resolve_tax_ids(self, tax_names: List[str]) -> List[int]:
        """
        Resolve tax IDs from tax names.
        
        Args:
            tax_names: List of tax names
        
        Returns:
            List of tax IDs
        """
        if not tax_names:
            return []
        
        placeholders = ','.join(['%s'] * len(tax_names))
        query = f"""
        SELECT id 
        FROM account_tax 
        WHERE name IN ({placeholders})
        """
        self.cursor.execute(query, tax_names)
        return [row[0] for row in self.cursor.fetchall()]
    
    def move_load_data(self, odoo_creds: Dict, query: str, project_name: str = 'dwh-project', **kwargs):
        """
        Main method to load account.move (invoices) into Odoo.
        
        Args:
            odoo_creds: Odoo credentials dictionary
            query: BigQuery query to fetch invoice data
            project_name: BigQuery project ID
            **kwargs: Additional context (e.g., task_instance)
        
        Returns:
            Number of successfully processed invoices
        """
        logger.info(f"Starting invoice load")
        
        # Fetch data from BigQuery
        client = bigquery.Client(project=project_name)
        query_job = client.query(query)
        results = query_job.result()
        
        logger.info(f"Retrieved {results.total_rows} invoices to process")
        
        error_list = []
        success_count = 0
        
        for row in results:
            try:
                # Check if invoice already exists (idempotency)
                existing_query = f"""
                SELECT id 
                FROM account_move 
                WHERE name = '{row['move_name']}'
                """
                existing_id = self.fetchone(existing_query)
                
                if existing_id:
                    logger.debug(f"Invoice {row['move_name']} already exists, skipping")
                    continue
                
                # Resolve foreign keys
                partner_id = self._resolve_partner_id(row)
                if not partner_id:
                    raise ValueError(f"Could not resolve partner for {row['partner_uuid']}")
                
                # Build invoice data
                invoice_data = {
                    'name': row['move_name'],
                    'partner_id': partner_id,
                    'invoice_date': row['invoice_date'],
                    'invoice_date_due': row['invoice_date_due'],
                    'move_type': row['move_type'],
                    'currency_id': row['currency_id'],
                    'journal_id': row['journal_id'],
                    'ref': row.get('reference'),
                    'invoice_line_ids': self._build_invoice_lines(row)
                }
                
                # Create invoice using OdooRPC
                move_model = self.odoo.env['account.move'].with_context(
                    skip_validation=False,
                    check_move_validity=True
                )
                
                invoice_id = move_model.create(invoice_data)
                
                # Create external ID for idempotency
                self.odoo.env['ir.model.data'].create({
                    'module': 'wsl_migration',
                    'model': 'account.move',
                    'res_id': invoice_id,
                    'name': row['move_name'],
                    'noupdate': True
                })
                
                logger.info(f"Created invoice {row['move_name']} with ID {invoice_id}")
                success_count += 1
                
            except Exception as e:
                logger.error(f"Failed to create invoice {row.get('move_name')}: {str(e)}")
                error_list.append({
                    'move_name': row.get('move_name'),
                    'error': str(e),
                    'partner_uuid': row.get('partner_uuid')
                })
                continue
        
        # Log summary
        logger.info(f"Invoice load complete: {success_count} succeeded, {len(error_list)} failed")
        
        # Write errors to GCS for investigation
        if error_list:
            task_id = kwargs.get('task_instance', {}).task_id if 'task_instance' in kwargs else 'unknown'
            self.write_list_to_gcs(error_list, f'accounts_error_list_{task_id}.csv')
        
        return success_count
    
    def _build_invoice_lines(self, row: Dict) -> List[tuple]:
        """
        Build invoice lines for account.move.
        
        Args:
            row: Invoice data from BigQuery
        
        Returns:
            List of command tuples for One2Many field:
            [(0, 0, line_data_1), (0, 0, line_data_2), ...]
        """
        lines = []
        
        # Get line items from BigQuery result
        for line in row.get('line_items', []):
            product_id = self._resolve_product_id(line['product_code'])
            tax_ids = self._resolve_tax_ids(line.get('tax_names', []))
            
            line_data = {
                'name': line['description'],
                'product_id': product_id,
                'quantity': line['quantity'],
                'price_unit': line['unit_price'],
                'tax_ids': [(6, 0, tax_ids)],  # Many2Many command tuple
                'account_id': line['account_id'],
            }
            
            # (0, 0, {...}) = Create new record
            lines.append((0, 0, line_data))
        
        return lines
    
    def write_list_to_gcs(
        self,
        error_list: List[Dict],
        file_name: str = 'accounts_error_list.csv',
        bucket_name: str = 'data-platform-bucket'
    ):
        """
        Write error list to Google Cloud Storage.
        
        Args:
            error_list: List of error dictionaries
            file_name: Name of file to create
            bucket_name: GCS bucket name
        """
        logger.info(f"Writing {len(error_list)} errors to GCS")
        
        try:
            storage_client = storage.Client()
            bucket = storage_client.bucket(bucket_name)
            blob_prefix = 'data/accounts_error_list/'
            blob = bucket.blob(blob_prefix + file_name)
            
            # Convert to CSV string
            import csv
            import io
            output = io.StringIO()
            if error_list:
                writer = csv.DictWriter(output, fieldnames=error_list[0].keys())
                writer.writeheader()
                writer.writerows(error_list)
            
            blob.upload_from_string(output.getvalue())
            logger.info(f"Error list written to gs://{bucket_name}/{blob_prefix}{file_name}")
            
        except Exception as e:
            logger.error(f"Failed to write error list to GCS: {str(e)}")
