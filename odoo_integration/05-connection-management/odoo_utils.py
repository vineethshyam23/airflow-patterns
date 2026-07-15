"""
Centralized connection management for Odoo integrations.

Used by all 116 Odoo DAGs for consistent connection handling.
Provides both OdooRPC (writes) and PostgreSQL (reads) connections.
"""

import time
import logging
from typing import Final
import urllib.error
import odoorpc
import psycopg2
from psycopg2 import Error

logger = logging.getLogger(__name__)


class Connection:
    """
    Centralized connection management for Odoo integrations.
    
    Provides:
    - OdooRPC connection for writes (respects business logic)
    - PostgreSQL connection for reads (10x faster)
    - Automatic retry logic
    - Proper timeout handling
    """
    
    @staticmethod
    def connect(odoo_creds):
        """
        Connect to Odoo via OdooRPC (XML-RPC).
        
        Args:
            odoo_creds: Dictionary containing:
                - hostname: Odoo server hostname
                - database: Database name
                - rpc_user: API user
                - rpc_pwd: API password
        
        Returns:
            odoorpc.ODOO: Connected Odoo client
        
        Raises:
            Exception: If connection fails
        
        Example:
            >>> odoo_creds = {
            ...     "hostname": "odoo.company.com",
            ...     "database": "production",
            ...     "rpc_user": "api_user",
            ...     "rpc_pwd": "api_password"
            ... }
            >>> conn = Connection()
            >>> odoo = conn.connect(odoo_creds)
            >>> odoo.env.user.name
            'API Access User'
        """
        endpoint = odoo_creds["hostname"]
        db_name = odoo_creds["database"]
        user = odoo_creds["rpc_user"]
        pwd = odoo_creds["rpc_pwd"]
        
        try:
            odoo_client = odoorpc.ODOO(
                endpoint,
                port=443,
                protocol="jsonrpc+ssl",  # Secure HTTPS
                version="16.0",
                timeout=30000,  # 30 second timeout
            )
            odoo_client.login(db_name, user, pwd)
            
            # Verify connection
            user_obj = odoo_client.env.user
            logger.info(f"Connected to Odoo as {user_obj.name}")
            logger.info(f"   Company: {user_obj.company_id.name}")
            
            return odoo_client
            
        except Exception as e:
            logger.error(f"Failed to connect to Odoo: {e}")
            raise e
    
    @staticmethod
    def connect_pg(odoo_creds):
        """
        Connect to Odoo PostgreSQL database directly.
        
        Used for fast read operations (lookups, aggregations, bulk queries).
        DO NOT use for writes - use OdooRPC to respect business logic.
        
        Args:
            odoo_creds: Dictionary containing:
                - hostname: Database hostname
                - database: Database name
                - db_user: Database user
                - db_pwd: Database password
        
        Returns:
            psycopg2.connection: PostgreSQL connection
        
        Raises:
            Exception: If connection fails
        
        Example:
            >>> conn = Connection()
            >>> pg_conn = conn.connect_pg(odoo_creds)
            >>> cursor = pg_conn.cursor()
            >>> cursor.execute("SELECT COUNT(*) FROM res_partner")
            >>> cursor.fetchone()
            (15234,)
        """
        try:
            # Disable wait callback for faster queries
            psycopg2.extensions.set_wait_callback(None)
            
            connection = psycopg2.connect(
                user=odoo_creds.get("db_user"),
                password=odoo_creds.get("db_pwd"),
                host=odoo_creds.get("hostname"),
                port=5432,
                database=odoo_creds.get("database"),
                sslmode="require",  # Force SSL
                connect_timeout=30000  # 30 second timeout
            )
            
            logger.info(f"Connected to PostgreSQL: {odoo_creds.get('hostname')}")
            
            return connection
            
        except (Exception, Error) as error:
            logger.error(f"Error connecting to PostgreSQL: {error}")
            raise error
    
    @staticmethod
    def run_with_retries(func: callable, *args, **kwargs):
        """
        Execute a function with automatic retry logic.
        
        Handles transient failures (timeouts, connection errors).
        
        Args:
            func: Function to execute
            *args, **kwargs: Arguments to pass to function
        
        Returns:
            Result of function execution
        
        Raises:
            RuntimeError: If all retries fail
        
        Example:
            >>> def create_partner(odoo, data):
            ...     return odoo.env['res.partner'].create(data)
            >>> 
            >>> result = Connection.run_with_retries(
            ...     create_partner, 
            ...     odoo, 
            ...     {'name': 'Test Partner'}
            ... )
        """
        _MAX_RETRIES: Final = 3
        _WAIT_TIME: Final = 60  # Wait 60 seconds between retries
        
        i = 1
        while i <= _MAX_RETRIES:
            try:
                result = func(*args, **kwargs)
                if i > 1:
                    logger.info(f"Retry {i-1} succeeded")
                return result
                
            except (odoorpc.error.RPCError, psycopg2.DatabaseError,
                    urllib.error.URLError, TimeoutError) as e:
                logger.warning(f"Attempt {i}/{_MAX_RETRIES} failed: {str(e)}")
                
                if i < _MAX_RETRIES:
                    logger.info(f"Waiting {_WAIT_TIME}s before retry...")
                    time.sleep(_WAIT_TIME)
                    i += 1
                    continue
                else:
                    raise RuntimeError(
                        f"All {_MAX_RETRIES} retries failed for {func.__name__}"
                    )
                    
            except Exception as e:
                # Non-retryable error, raise immediately
                logger.error(f"Non-retryable error: {str(e)}")
                raise e
        
        raise RuntimeError(f"Unexpected: exited retry loop for {func.__name__}")
