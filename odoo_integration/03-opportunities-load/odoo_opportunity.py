"""
Opportunity load with dataclass-based type safety and OdooDefaults caching.

Demonstrates:
- Dataclass for type-safe data modeling
- OdooDefaults caching for performance
- Dual connection strategy
- ntile-based batching
"""

from dataclasses import dataclass
from typing import Optional, Any, List, Dict
import logging
import odoorpc
import psycopg2
from google.cloud import bigquery

logger = logging.getLogger(__name__)


@dataclass
class OdooCredentials:
    """Odoo connection credentials."""
    hostname: str
    db_name: str
    api_user: str
    api_passwd: str
    db_user: str
    db_passwd: str
    api_port: int = 443
    db_port: int = 5432


@dataclass
class OdooDefaults:
    """
    Cache Odoo default values for performance.
    
    Loaded once at class initialization, reused for all records.
    """
    salutations: dict  # {'Mr.': 1, 'Ms.': 2, ...}
    utm_medium: dict  # {'Email': 1, 'Phone': 2, ...}
    utm_source: dict  # {'Website': 1, 'Referral': 2, ...}
    establishment_type: dict  # {'restaurant': 1, 'cafe': 2, ...}
    dish_metro_store: dict  # {'MCCDE': 1, 'MCCES': 2, ...}
    res_country: dict  # {'DE': 1, 'ES': 2, ...}
    res_partner: dict  # {(uuid, is_company, type): id}
    res_partner_comp: dict  # {(name_hash, uuid): id}
    ir_model_data: dict  # {'external_id': id}


class Data:
    """
    Handles opportunity data operations with caching and type safety.
    """
    
    def __init__(
        self, 
        run_interval: str, 
        query_name: str, 
        tiles: int, 
        odoo_credentials: OdooCredentials
    ):
        logger.info("Initializing Opportunity Data class")
        
        # Odoo connections
        self.odoo_credentials = odoo_credentials
        self.odoo = odoorpc.ODOO(
            self.odoo_credentials.hostname,
            port=self.odoo_credentials.api_port,
            protocol="jsonrpc+ssl",
            version="16",
        )
        self.odoo.login(
            self.odoo_credentials.db_name,
            self.odoo_credentials.api_user,
            self.odoo_credentials.api_passwd,
        )
        
        # PostgreSQL connection
        self.pg_connection = psycopg2.connect(
            user=self.odoo_credentials.db_user,
            password=self.odoo_credentials.db_passwd,
            host=self.odoo_credentials.hostname,
            port=self.odoo_credentials.db_port,
            database=self.odoo_credentials.db_name,
            sslmode="require",
            connect_timeout=30000
        )
        self.cursor = self.pg_connection.cursor()
        
        # BigQuery client
        self.bq_client = bigquery.Client()
        
        # Models with contexts
        self.partner_model = self.odoo.env["res.partner"].with_context(
            no_vat_validation=True,
            skip_dish_account_sync=True
        )
        self.ir_data_model = self.odoo.env["ir.model.data"]
        
        # Load and cache defaults
        self.defaults: OdooDefaults = self.get_odoo_defaults(
            run_interval, query_name, tiles
        )
    
    def get_odoo_defaults(
        self, 
        run_interval: str, 
        query_name: str, 
        tiles: int
    ) -> OdooDefaults:
        """
        Load Odoo defaults once and cache.
        
        Args:
            run_interval: For daily loads (e.g., "1 day")
            query_name: "customer_daily_load" or "customer_full_load_ntiles"
            tiles: For full loads, which ntile batch (1-10)
        
        Returns:
            OdooDefaults dataclass with all cached values
        """
        logger.info("Loading Odoo defaults...")
        
        # Get data from BigQuery
        if query_name == "customer_daily_load":
            query = f"""
            SELECT * FROM `dwh_trusted.int_opportunities`
            WHERE DATE(last_modified) >= CURRENT_DATE() - INTERVAL {run_interval}
            """
        elif query_name == "customer_full_load_ntiles":
            query = f"""
            SELECT * FROM `dwh_trusted.int_opportunities`
            WHERE NTILE(10) OVER (ORDER BY id) = {tiles}
            """
        
        query_job = self.bq_client.query(query)
        accs = query_job.result()
        self.accs_list = [dict(row) for row in accs]
        
        # Extract unique IDs for bulk lookups
        distinct_account_ids = list({d["account_id"] for d in self.accs_list})
        
        # Load all reference data in bulk
        # Get salutations
        self.cursor.execute("SELECT id, name->>'en_US' AS name FROM res_partner_title")
        salutations = {name: id for id, name in self.cursor.fetchall()}
        
        # Get UTM medium
        self.cursor.execute("SELECT id, name FROM utm_medium")
        utm_medium = {name: id for id, name in self.cursor.fetchall()}
        
        # Get UTM source
        self.cursor.execute("SELECT id, name FROM utm_source")
        utm_source = {name: id for id, name in self.cursor.fetchall()}
        
        # Get establishment types
        self.cursor.execute("""
            SELECT id, LOWER(name) as name FROM establishment_type
        """)
        establishment_type = {name: id for id, name in self.cursor.fetchall()}
        
        # Get countries
        self.cursor.execute("SELECT id, code FROM res_country")
        res_country = {code: id for id, code in self.cursor.fetchall()}
        
        # More cache initialization here...
        
        return OdooDefaults(
            salutations=salutations,
            utm_medium=utm_medium,
            utm_source=utm_source,
            establishment_type=establishment_type,
            dish_metro_store={},
            res_country=res_country,
            res_partner={},
            res_partner_comp={},
            ir_model_data={},
        )
    
    def load_opportunities(self):
        """
        Main method to load opportunities into Odoo.
        
        Uses cached defaults for performance.
        """
        logger.info(f"Loading {len(self.accs_list)} opportunities")
        
        success_count = 0
        error_count = 0
        
        for acc in self.accs_list:
            try:
                # Build opportunity data using cached defaults
                opp_data = {
                    'name': acc['opportunity_name'],
                    'partner_id': acc.get('company_id'),
                    'type': 'opportunity',
                    'stage_id': acc.get('stage_id'),
                    'user_id': acc.get('user_id'),
                    'expected_revenue': acc.get('expected_revenue'),
                    'probability': acc.get('probability'),
                    'date_deadline': acc.get('close_date'),
                    'title_id': self.defaults.salutations.get(acc.get('salutation')),
                    'medium_id': self.defaults.utm_medium.get(acc.get('utm_medium')),
                    'source_id': self.defaults.utm_source.get(acc.get('utm_source')),
                    'country_id': self.defaults.res_country.get(acc.get('country_code')),
                }
                
                # Filter None values
                opp_data = {k: v for k, v in opp_data.items() if v is not None}
                
                # Create opportunity
                opp_id = self.odoo.env['crm.lead'].create(opp_data)
                
                logger.info(f"✅ Created opportunity {acc['opportunity_name']} with ID {opp_id}")
                success_count += 1
                
            except Exception as e:
                logger.error(f"❌ Failed: {acc.get('opportunity_id')}: {e}")
                error_count += 1
                continue
        
        logger.info(f"Complete: {success_count} succeeded, {error_count} failed")
        return success_count
