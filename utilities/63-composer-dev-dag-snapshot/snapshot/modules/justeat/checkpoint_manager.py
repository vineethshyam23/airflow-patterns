#!/usr/bin/env python3
"""
Checkpoint Manager for JustEat Spider
Provides utilities for managing checkpoint functionality including:
- Clearing checkpoints
- Checking zipcode status
- Getting failed zipcodes
- Managing scraping status
"""

import psycopg2
import logging
import os
from typing import List, Optional, Dict, Any
from . import settings

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class CheckpointManager:
    """
    Comprehensive checkpoint management for the JustEat spider.
    
    This class provides full checkpoint management functionality including:
    - Checking zipcode processing status
    - Getting failed zipcodes
    - Clearing checkpoints
    - Getting processing statistics
    - Marking zipcodes as failed
    
    Note: The PostgresPipeline class also has lightweight versions of some methods
    for use during spider execution. This CheckpointManager is intended for
    comprehensive management operations outside of spider execution.
    """
    
    def __init__(self, host: str = None, database: str = None, user: str = None, password: str = None, port: str = None):
        """
        Initialize checkpoint manager with database connection details.
        If no parameters are provided, uses configuration from settings.py
        
        Args:
            host: Database host (optional, defaults to settings)
            database: Database name (optional, defaults to settings)
            user: Database user (optional, defaults to settings)
            password: Database password (optional, defaults to settings)
            port: Database port (optional, defaults to settings)
        """
        self.host = host or settings.POSTGRES_HOST
        self.database = database or settings.POSTGRES_DATABASE
        self.user = user or settings.POSTGRES_USER
        self.password = password or settings.POSTGRES_PASSWORD
        self.port = port or settings.POSTGRES_PORT
        self.connection = None
        self.table_name = settings.POSTGRES_TABLE_NAME
    
    def connect(self):
        """Establish database connection."""
        try:
            self.connection = psycopg2.connect(
                host=self.host,
                database=self.database,
                user=self.user,
                password=self.password,
                port=self.port,
                connect_timeout=settings.POSTGRES_CONNECTION_TIMEOUT
            )
            self.connection.autocommit = True
            logger.info(f"✅ Connected to database {self.database} on {self.host}")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to connect to database: {e}")
            return False
    
    def disconnect(self):
        """Close database connection."""
        if self.connection:
            self.connection.close()
            logger.info("🔌 Database connection closed")
    
    def is_zipcode_processed(self, zipcode: str) -> bool:
        """
        Check if a zipcode has been successfully processed.
        
        Args:
            zipcode: The zipcode to check
            
        Returns:
            True if zipcode has been successfully processed, False otherwise
        """
        if not self.connection:
            logger.error("❌ No database connection")
            return False
            
        try:
            with self.connection.cursor() as cursor:
                query = f"""
                SELECT COUNT(*) as count
                FROM {self.table_name}
                WHERE zipcode = %s AND scraping_status = 'success'
                """
                cursor.execute(query, (zipcode,))
                result = cursor.fetchone()
                count = result[0] if result else 0
                
                if count > 0:
                    logger.info(f"✅ Zipcode {zipcode} already processed successfully")
                    return True
                else:
                    logger.info(f"🔄 Zipcode {zipcode} not yet processed")
                    return False
                    
        except Exception as e:
            logger.error(f"❌ Error checking zipcode {zipcode}: {e}")
            return False
    
    def get_failed_zipcodes(self) -> List[str]:
        """
        Get list of zipcodes that failed processing.
        
        Returns:
            List of failed zipcodes
        """
        if not self.connection:
            logger.error("❌ No database connection")
            return []
            
        try:
            with self.connection.cursor() as cursor:
                query = f"""
                SELECT DISTINCT zipcode
                FROM {self.table_name}
                WHERE scraping_status = 'failed' AND zipcode IS NOT NULL
                ORDER BY zipcode
                """
                cursor.execute(query)
                results = cursor.fetchall()
                failed_zipcodes = [row[0] for row in results]
                
                logger.info(f"📋 Found {len(failed_zipcodes)} failed zipcodes")
                return failed_zipcodes
                
        except Exception as e:
            logger.error(f"❌ Error getting failed zipcodes: {e}")
            return []
    
    def get_zipcode_stats(self) -> Dict[str, int]:
        """
        Get statistics about zipcode processing status.
        
        Returns:
            Dictionary with status counts
        """
        if not self.connection:
            logger.error("❌ No database connection")
            return {}
            
        try:
            with self.connection.cursor() as cursor:
                query = f"""
                SELECT scraping_status, COUNT(DISTINCT zipcode) as count
                FROM {self.table_name}
                WHERE zipcode IS NOT NULL
                GROUP BY scraping_status
                """
                cursor.execute(query)
                results = cursor.fetchall()
                
                stats = {}
                for status, count in results:
                    stats[status] = count
                
                logger.info(f"📊 Zipcode processing stats: {stats}")
                return stats
                
        except Exception as e:
            logger.error(f"❌ Error getting zipcode stats: {e}")
            return {}
    
    def clear_zipcode_checkpoints(self, zipcode: Optional[str] = None) -> bool:
        """
        Clear checkpoint records for a specific zipcode or all zipcodes.
        
        Args:
            zipcode: Specific zipcode to clear, or None to clear all
            
        Returns:
            True if successful, False otherwise
        """
        if not self.connection:
            logger.error("❌ No database connection")
            return False
            
        try:
            with self.connection.cursor() as cursor:
                if zipcode:
                    query = f"""
                    DELETE FROM {self.table_name}
                    WHERE zipcode = %s AND scraping_status IN ('pending', 'success', 'failed')
                    """
                    cursor.execute(query, (zipcode,))
                    logger.info(f"🗑️ Cleared checkpoints for zipcode {zipcode}")
                else:
                    query = f"""
                    DELETE FROM {self.table_name}
                    WHERE scraping_status IN ('pending', 'success', 'failed')
                    """
                    cursor.execute(query)
                    logger.info(f"🗑️ Cleared all checkpoint records")
                
                return True
                
        except Exception as e:
            logger.error(f"❌ Error clearing checkpoints: {e}")
            return False
    
    def mark_zipcode_as_failed(self, zipcode: str, error_message: str = None) -> bool:
        """
        Mark a zipcode as failed.
        
        Args:
            zipcode: The zipcode to mark as failed
            error_message: Optional error message
            
        Returns:
            True if successful, False otherwise
        """
        if not self.connection:
            logger.error("❌ No database connection")
            return False
            
        try:
            with self.connection.cursor() as cursor:
                from datetime import datetime
                import json
                
                current_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                query = f"""
                INSERT INTO {self.table_name} 
                (spider, jsondata, eingefuegtam, eingefuegtvon, geaendertam, geaendertvon, 
                 zipcode, latitude, longitude, rest_id, scraping_status) 
                VALUES 
                (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (spider, md5(jsondata::text)) DO NOTHING
                """
                
                error_data = {
                    'error_message': error_message or 'Unknown error',
                    'zipcode': zipcode,
                    'failed_at': current_timestamp
                }
                
                cursor.execute(query, (
                    'checkpoint_manager',
                    json.dumps(error_data),
                    current_timestamp,
                    'checkpoint_manager',
                    current_timestamp,
                    'checkpoint_manager',
                    zipcode,
                    None,
                    None,
                    '',
                    'failed'
                ))
                
                logger.info(f"❌ Marked zipcode {zipcode} as failed")
                return True
                
        except Exception as e:
            logger.error(f"❌ Error marking zipcode {zipcode} as failed: {e}")
            return False




def create_checkpoint_manager() -> Optional[CheckpointManager]:
    """
    Create a checkpoint manager with configuration from settings.py.
    
    Returns:
        CheckpointManager instance or None if configuration not available
    """
    # Check if all required settings are available
    if not all([settings.POSTGRES_HOST, settings.POSTGRES_DATABASE, 
                settings.POSTGRES_USER, settings.POSTGRES_PASSWORD]):
        logger.error("❌ Database configuration not available in settings")
        return None
    
    manager = CheckpointManager()
    if manager.connect():
        return manager
    else:
        return None


def clear_all_checkpoints() -> bool:
    """
    Clear all checkpoint records.
    
    Returns:
        True if successful, False otherwise
    """
    manager = create_checkpoint_manager()
    if not manager:
        return False
    
    try:
        result = manager.clear_zipcode_checkpoints()
        return result
    finally:
        manager.disconnect()


def clear_zipcode_checkpoints(zipcode: str) -> bool:
    """
    Clear checkpoint records for a specific zipcode.
    
    Args:
        zipcode: The zipcode to clear
        
    Returns:
        True if successful, False otherwise
    """
    manager = create_checkpoint_manager()
    if not manager:
        return False
    
    try:
        result = manager.clear_zipcode_checkpoints(zipcode)
        return result
    finally:
        manager.disconnect()


def get_zipcode_processing_stats() -> Dict[str, int]:
    """
    Get zipcode processing statistics.
    
    Returns:
        Dictionary with processing statistics
    """
    manager = create_checkpoint_manager()
    if not manager:
        return {}
    
    try:
        return manager.get_zipcode_stats()
    finally:
        manager.disconnect()


def get_failed_zipcodes_list() -> List[str]:
    """
    Get list of failed zipcodes.
    
    Returns:
        List of failed zipcodes
    """
    manager = create_checkpoint_manager()
    if not manager:
        return []
    
    try:
        return manager.get_failed_zipcodes()
    finally:
        manager.disconnect()


if __name__ == "__main__":
    """Command line interface for checkpoint management."""
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python checkpoint_manager.py <command> [args]")
        print("Commands:")
        print("  clear-all                    - Clear all checkpoints")
        print("  clear-zipcode <zipcode>      - Clear checkpoints for specific zipcode")
        print("  stats                        - Show processing statistics")
        print("  failed                       - List failed zipcodes")
        print("  check <zipcode>              - Check if zipcode is processed")
        sys.exit(1)
    
    command = sys.argv[1]
    
    if command == "clear-all":
        success = clear_all_checkpoints()
        print(f"Clear all checkpoints: {'✅ Success' if success else '❌ Failed'}")
    
    elif command == "clear-zipcode":
        if len(sys.argv) < 3:
            print("❌ Please provide zipcode")
            sys.exit(1)
        zipcode = sys.argv[2]
        success = clear_zipcode_checkpoints(zipcode)
        print(f"Clear checkpoints for {zipcode}: {'✅ Success' if success else '❌ Failed'}")
    
    elif command == "stats":
        stats = get_zipcode_processing_stats()
        print("📊 Zipcode Processing Statistics:")
        for status, count in stats.items():
            print(f"  {status}: {count}")
    
    elif command == "failed":
        failed_zipcodes = get_failed_zipcodes_list()
        print(f"❌ Failed zipcodes ({len(failed_zipcodes)}):")
        for zipcode in failed_zipcodes:
            print(f"  {zipcode}")
    
    elif command == "check":
        if len(sys.argv) < 3:
            print("❌ Please provide zipcode")
            sys.exit(1)
        zipcode = sys.argv[2]
        manager = create_checkpoint_manager()
        if manager:
            is_processed = manager.is_zipcode_processed(zipcode)
            print(f"Zipcode {zipcode}: {'✅ Processed' if is_processed else '🔄 Not processed'}")
            manager.disconnect()
        else:
            print("❌ Could not connect to database")
    
    else:
        print(f"❌ Unknown command: {command}")
        sys.exit(1)
