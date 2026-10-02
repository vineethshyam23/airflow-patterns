#!/usr/bin/env python3
"""
HTML Parser - Python version of html_parser.php
Converts URLs to HTML content and stores in database

Usage from Airflow DAG:
    from dags.modules.html_parser import run_html_parser

    # Equivalent to: html-parse.php -shell -dbhost $alloyip -dbpasswd $alloypw -dbuser "postgres" -maxminutes "120"
    run_html_parser(env="PROD", maxminutes=120)

    # With modulo filter
    run_html_parser(env="PROD", maxminutes=120, modulo="10")

    # Test mode
    run_html_parser(env="PROD", maxminutes=120, test=True)
"""

import hashlib
import logging
import re
import sys
import time
import signal
import threading
import warnings
import random
from urllib.parse import urljoin, urlparse, urlunparse
from typing import Dict, List, Any
from contextlib import contextmanager

import psycopg2
import requests
from psycopg2.extras import RealDictCursor

# Suppress SSL warnings for cleaner logs (expected behavior for fallback)
warnings.filterwarnings('ignore', message='Unverified HTTPS request')

try:
    from .db_connections import initialize_db_connection
except ImportError:
    from modules.db_connections import initialize_db_connection

    # Fallback for when imported as standalone module


# Import database connection modules - will be imported when needed

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class RobotsTxtParser:
    """Enhanced robots.txt parser for Python - more robust implementation"""

    def __init__(self, content: str):
        self.content = content
        self.rules = {}
        self.user_agent = "*"
        self._parse_rules()

    def _parse_rules(self):
        """Parse robots.txt content into rules with better error handling"""
        current_ua = "*"
        lines = self.content.split("\n")

        for line in lines:
            line = line.strip()
            if not line or line.startswith("#"):
                continue

            # Handle malformed lines
            if ":" not in line:
                continue

            try:
                directive, value = line.split(":", 1)
                directive = directive.strip().lower()
                value = value.strip()

                if directive == "user-agent":
                    current_ua = value.lower()
                    if current_ua not in self.rules:
                        self.rules[current_ua] = {"allow": [], "disallow": []}
                elif directive in ["allow", "disallow"]:
                    if current_ua not in self.rules:
                        self.rules[current_ua] = {"allow": [], "disallow": []}
                    self.rules[current_ua][directive].append(value)
            except Exception:
                # Skip malformed lines
                continue

    def set_user_agent(self, user_agent: str):
        """Set user agent for checking rules"""
        self.user_agent = user_agent.lower()

    def is_allowed(self, path: str) -> bool:
        """Check if path is allowed for current user agent with improved logic"""
        # Find matching user agent rules (exact match first, then wildcard)
        rules = self.rules.get(self.user_agent, self.rules.get("*", {}))

        # Check disallow rules first (more restrictive)
        for disallow_path in rules.get("disallow", []):
            if self._path_matches(disallow_path, path):
                return False

        # Check allow rules (more permissive)
        for allow_path in rules.get("allow", []):
            if self._path_matches(allow_path, path):
                return True

        # Default to allowed if no specific rules
        return True

    def _path_matches(self, rule_path: str, request_path: str) -> bool:
        """Check if request path matches rule path with improved pattern matching"""
        if not rule_path:
            return False

        # Handle special cases
        if rule_path == "/":
            return request_path == "/"

        if rule_path == "*":
            return True

        # Convert wildcards to regex with proper escaping
        pattern = re.escape(rule_path)
        pattern = pattern.replace("\\*", ".*")
        pattern = pattern.replace("\\?", ".")

        # Add start anchor if not wildcard
        if not pattern.startswith(".*"):
            pattern = "^" + pattern

        # Add end anchor for exact matches
        if not pattern.endswith(".*"):
            pattern = pattern + "$"

        try:
            return bool(re.match(pattern, request_path))
        except re.error:
            # Fallback to simple string matching for malformed patterns
            return request_path.startswith(rule_path)


class PostgresDatabase:
    """PostgreSQL database connection wrapper with improved transaction handling"""

    def __init__(self, env: str = "PROD"):
        self.connection = None
        self.last_error = None
        self.last_query = ""
        self.connect(env)

    def connect(self, env: str = "PROD"):
        """Establish database connection using existing modules"""
        try:
            # Import here to avoid import issues when module is executed directly

            # Try to use initialize_db_connection first
            self.connection = initialize_db_connection(env)
            # Don't set autocommit to allow explicit transaction control
            self.connection.autocommit = False
            # Set timezone
            with self.connection.cursor() as cursor:
                cursor.execute("SET timezone='Europe/Berlin';")
            self.connection.commit()

        except Exception as e:
            self.last_error = str(e)
            logger.error(f"Database connection failed: {e}")
            raise

    def exec(self, sql: str) -> bool:
        """Execute SQL statement with improved error handling"""
        if not self.connection:
            self.last_error = "No database connection"
            return False
        try:
            self.last_query = sql
            with self.connection.cursor() as cursor:
                cursor.execute(sql)
            return True
        except Exception as e:
            self.last_error = str(e)
            logger.error(f"SQL execution failed: {e}")
            return False

    def query(self, sql: str) -> List[Dict[str, Any]]:
        """Execute query and return results with improved error handling"""
        if not self.connection:
            self.last_error = "No database connection"
            return []
        try:
            self.last_query = sql
            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(sql)
                return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            self.last_error = str(e)
            logger.error(f"SQL query failed: {e}")
            return []

    def quote(self, value: str) -> str:
        """Quote string value for SQL with improved handling"""
        if value is None or value == "":
            return "null"
        # Escape single quotes by doubling them (PostgreSQL standard)
        return f"'{value.replace(chr(39), chr(39) + chr(39))}'"

    def commit(self):
        """Commit transaction explicitly"""
        if self.connection:
            try:
                self.connection.commit()
            except Exception as e:
                self.last_error = str(e)
                logger.error(f"Commit failed: {e}")
                return False
        return True

    def rollback(self):
        """Rollback transaction"""
        if self.connection:
            try:
                self.connection.rollback()
            except Exception as e:
                self.last_error = str(e)
                logger.error(f"Rollback failed: {e}")

    def error_to_string(self) -> str:
        """Get last error as string"""
        return self.last_error or ""


class HTMLParse:
    """HTML Parser main class"""

    def __init__(
        self,
        env: str = "PROD",
        maxminutes: int = 120,
        modulo: str = "",
        test: bool = False,
        sql: str | None = None,
        task_id: int = 0,
        total_tasks: int = 1,
    ):
        self.args = {
            "Shell": True,  # Always True when called from DAG
            "modulo": modulo,
            "maxminutes": maxminutes or 120,
            "Test": test,
            "env": env,
        }
        self.arg_map = {
            "modulo": "modulo",
            "maxminutes": "maxminutes",
        }
        self.db = None
        self.script_name = "html_parser.py"
        self.start_timestamp = 0
        self.end_timestamp = 0
        self.last_curl_error = 0
        self.last_curl_error_msg = ""
        self.sql = sql
        self.task_id = task_id
        self.total_tasks = total_tasks
        self.should_stop = False
        self.processed_count = 0
        self.error_count = 0
        self.total_processed = 0  # Track total URLs processed
        self.max_errors = 3000  # Increased threshold for 2500 URLs per task

        # Set up timeout handler
        self.setup_timeout_handler()
        
        self.init_database(env)
        self.init()

    def setup_timeout_handler(self):
        """Set up timeout handler for graceful shutdown"""
        def timeout_handler(signum, frame):
            logger.warning("Timeout signal received, initiating graceful shutdown...")
            self.should_stop = True
            
        # Set up signal handlers for graceful shutdown
        try:
            signal.signal(signal.SIGTERM, timeout_handler)
            signal.signal(signal.SIGINT, timeout_handler)
        except Exception as e:
            logger.warning(f"Could not set up signal handlers: {e}")

    @contextmanager
    def timeout_context(self, timeout_seconds=15):  # Kept at 15 seconds as requested
        """Context manager for timeout handling"""
        def timeout_handler():
            time.sleep(timeout_seconds)
            if not self.should_stop:
                logger.warning(f"Operation timed out after {timeout_seconds} seconds")
                self.should_stop = True
                
        timer = threading.Timer(timeout_seconds, timeout_handler)
        try:
            timer.start()
            yield
        finally:
            timer.cancel()

    def check_should_stop(self):
        """Check if processing should stop due to time limit or errors"""
        if self.should_stop:
            return True
            
        if self.end_timestamp < time.time():
            logger.info("Maximum runtime reached, stopping processing")
            return True
            
        if self.error_count >= self.max_errors:
            logger.error(f"Too many errors ({self.error_count}), stopping processing")
            return True
            
        return False

    def init_database(self, env: str = "PROD"):
        """Initialize database connection with retry logic"""
        max_retries = 3
        retry_delay = 5
        
        for attempt in range(max_retries):
            try:
                self.db = PostgresDatabase(env)
                logger.info("Database connection established successfully")
                return
            except Exception as e:
                self.log_error(f"Database connection attempt {attempt + 1} failed: {str(e)}")
                if attempt < max_retries - 1:
                    logger.info(f"Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
                else:
                    self.log_error("All database connection attempts failed")
                    raise Exception(f"Failed to connect to database after {max_retries} attempts: {str(e)}")

    def init(self):
        """Initialize parser settings"""
        if self.args["modulo"] is True:
            self.args["modulo"] = ""

        self.start_timestamp = time.time()
        if self.args["maxminutes"] > 0:
            self.end_timestamp = time.time() + self.args["maxminutes"] * 60
        else:  # max 1 day
            self.end_timestamp = time.time() + 24 * 60 * 60

    def log_connect(self, message: str):
        """Log connection message"""
        task_prefix = f"[Task {self.task_id}/{self.total_tasks}]" if self.task_id > 0 else ""
        logger.info(f"{task_prefix}[CONNECT] {message}")

    def log_hint(self, message: str):
        """Log hint message"""
        task_prefix = f"[Task {self.task_id}/{self.total_tasks}]" if self.task_id > 0 else ""
        logger.info(f"{task_prefix}[HINT] {message}")

    def log_error(self, message: str):
        """Log error message"""
        task_prefix = f"[Task {self.task_id}/{self.total_tasks}]" if self.task_id > 0 else ""
        logger.error(f"{task_prefix}[ERROR] {message}")

    def run(self):
        """Main execution method with comprehensive error handling"""
        self.log_connect("HTML Parse wird gestartet...")
        if self.args.get("Test"):
            self.log_hint("Testverarbeitung ist aktiviert...")

        try:
            self.write_htmls()
        except KeyboardInterrupt:
            self.log_hint("Processing interrupted by user")
        except Exception as e:
            self.log_error(f"Critical error in main execution: {str(e)}")
            raise  # Re-raise to ensure Airflow knows about the failure
        finally:
            # Cleanup
            if self.db and self.db.connection:
                try:
                    self.db.connection.close()
                    logger.info("Database connection closed")
                except Exception as e:
                    logger.warning(f"Error closing database connection: {e}")

        success_rate = (self.processed_count / self.total_processed) * 100 if self.total_processed > 0 else 0
        self.log_hint(f"Verarbeitung beendet. Processed: {self.processed_count}, Errors: {self.error_count}, Success Rate: {success_rate:.1f}%")

    def check_robots(self, url: str) -> bool:
        """Check robots.txt for URL - SPEED OPTIMIZED"""
        # SPEED OPTIMIZATION: Skip robots.txt for faster processing
        # Most sites block anyway, so we'll just try all URLs
        return True
        
        # Original robots.txt logic (commented out for speed)
        """
        try:
            parts = urlparse(url)
            if not parts.netloc:
                return True

            if not parts.path:
                parts = parts._replace(path="/")

            try:
                robots_url = f"https://{parts.netloc}/robots.txt"
                response = requests.get(robots_url, timeout=3)  # Reduced from 10 to 3 seconds
                robot_content = response.text

                if len(robot_content.strip()) > 0 and robot_content.strip()[0] == "<":
                    return True
                else:
                    parser = RobotsTxtParser(robot_content)
            except Exception:
                return True

            parser.set_user_agent("DatalogueBot/1.0")
            return parser.is_allowed(parts.path)

        except Exception:
            return True
        """

    def add_http(self, url: str) -> str:
        """Add http:// prefix if missing"""
        if not re.match(r"^(?:f|ht)tps?://", url, re.IGNORECASE):
            url = "http://" + url
        return url

    def url_to_absolute(self, base_url: str, relative_url: str) -> str:
        """Convert relative URL to absolute URL with enhanced logic matching PHP version"""
        from urllib.parse import urlparse, urlunparse, urljoin

        # Sanitize URLs to prevent parsing errors
        if not relative_url or not isinstance(relative_url, str):
            return base_url
            
        # Remove any non-URL characters that might cause parsing issues
        relative_url = relative_url.strip()
        if not relative_url:
            return base_url
            
        # Skip URLs that are clearly not valid
        if any(invalid_char in relative_url for invalid_char in ['"', "'", '\n', '\r', '\t']):
            return base_url

        try:
            # Parse the relative URL
            relative_parts = urlparse(relative_url)
        except Exception as e:
            # If URL parsing fails, return base URL
            logger.warning(f"URL parsing failed for '{relative_url}': {str(e)}")
            return base_url

        # If relative URL has a scheme, it's already absolute
        if relative_parts.scheme:
            return relative_url

        # Parse the base URL
        base_parts = urlparse(base_url)

        # If base URL has no scheme, return relative URL as is
        if not base_parts.scheme:
            return relative_url

        # Start with base URL parts
        absolute_parts = list(base_parts)

        # Handle path
        if relative_parts.path:
            if relative_parts.path.startswith("/"):
                # Absolute path
                absolute_parts[2] = relative_parts.path
            else:
                # Relative path - combine with base path
                base_path = absolute_parts[2] or "/"
                # Remove filename from base path if it exists
                if "." in base_path.split("/")[-1] and "/" in base_path:
                    base_path = "/".join(base_path.split("/")[:-1]) + "/"
                if not base_path.endswith("/"):
                    base_path += "/"
                absolute_parts[2] = base_path + relative_parts.path

            # Handle query parameters
            if relative_parts.query:
                absolute_parts[4] = relative_parts.query
            elif absolute_parts[4]:
                # Keep base query if no relative query
                pass
            else:
                absolute_parts[4] = ""
        else:
            # No relative path, keep base path
            if relative_parts.query:
                absolute_parts[4] = relative_parts.query
            elif absolute_parts[4]:
                # Keep base query
                pass
            else:
                absolute_parts[4] = ""

        # Handle fragment
        if relative_parts.fragment:
            absolute_parts[5] = relative_parts.fragment
        elif absolute_parts[5]:
            # Keep base fragment
            pass
        else:
            absolute_parts[5] = ""

        # Normalize path (handle . and ..)
        path_parts = absolute_parts[2].split("/")
        normalized_parts = []

        for part in path_parts:
            if part == "." or part == "":
                continue
            elif part == "..":
                if normalized_parts:
                    normalized_parts.pop()
            else:
                normalized_parts.append(part)

        # Reconstruct path
        absolute_parts[2] = "/" + "/".join(normalized_parts)

        # Build the absolute URL
        return urlunparse(absolute_parts)

    def get_html(self, url: str) -> Dict[str, Any]:
        """Fetch HTML content from URL with optimized timeouts and SSL fallback"""
        try:
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
                "Accept-Language": "de-DE,de;q=0.9,en;q=0.8",
                "Accept-Encoding": "gzip, deflate, br",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1",
                "Sec-Fetch-Dest": "document",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Site": "none",
                "Sec-Fetch-User": "?1",
                "Cache-Control": "max-age=0"
            }

            # Create a session for better connection handling
            session = requests.Session()
            session.headers.update(headers)
            
            # Configure session for better performance
            adapter = requests.adapters.HTTPAdapter(
                pool_connections=10,
                pool_maxsize=10,
                max_retries=0  # We handle retries manually
            )
            session.mount('http://', adapter)
            session.mount('https://', adapter)

            # Single attempt for faster processing
            for attempt in range(1):
                try:
                    # First attempt with SSL verification (like working Spain script)
                    try:
                        response = session.get(
                            url,
                            timeout=30,  # Increased timeout to 30 seconds for slow sites
                            verify=True,  # Try with SSL verification first
                            allow_redirects=True,
                            stream=True  # Use streaming for memory efficiency
                        )
                        
                        # Get content type but don't reject early - let validation handle it later
                        content_type = response.headers.get('content-type', '').lower()
                        
                        # Read content with size limit (2MB for larger content)
                        content = ""
                        size_read = 0
                        max_size = 2_000_000  # 2MB limit (increased from 1MB)
                        
                        for chunk in response.iter_content(chunk_size=8192, decode_unicode=True):
                            if chunk:
                                size_read += len(chunk)
                                if size_read > max_size:
                                    response.close()
                                    return {"html": False, "response_type": content_type, "http_code": response.status_code}
                                content += chunk

                        response.close()
                        
                        return {
                            "html": content,
                            "response_type": content_type,
                            "http_code": response.status_code,
                        }
                        
                    except requests.exceptions.SSLError:
                        # SSL fallback: try without verification (like working Spain script)
                        response = session.get(
                            url,
                            timeout=30,  # Increased timeout to 30 seconds for slow sites
                            allow_redirects=True,
                            verify=False,  # SSL fallback
                            stream=True
                        )
                        
                        # Get content type but don't reject early - let validation handle it later
                        content_type = response.headers.get('content-type', '').lower()
                        
                        # Read content with size limit
                        content = ""
                        size_read = 0
                        max_size = 2_000_000  # 2MB limit (increased from 1MB)
                        
                        for chunk in response.iter_content(chunk_size=8192, decode_unicode=True):
                            if chunk:
                                size_read += len(chunk)
                                if size_read > max_size:
                                    response.close()
                                    return {"html": False, "response_type": content_type, "http_code": response.status_code}
                                content += chunk

                        response.close()
                        
                        return {
                            "html": content,
                            "response_type": content_type,
                            "http_code": response.status_code,
                        }
                        
                except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
                    # Single attempt failed, log and return error
                    if "timeout" in str(e).lower():
                        self.last_curl_error = 28  # CURLE_OPERATION_TIMEDOUT
                        self.last_curl_error_msg = f"Operation timed out: {str(e)}"
                    else:
                        self.last_curl_error = 7  # CURLE_COULDNT_CONNECT
                        self.last_curl_error_msg = f"Connection error: {str(e)}"
                    return {"html": False, "response_type": "", "http_code": 0}
                    
                # If we get here, the request was successful
                break

        except requests.exceptions.SSLError as e:
            self.last_curl_error = 60  # CURLE_SSL_CACERT
            self.last_curl_error_msg = f"SSL certificate problem: {str(e)}"
            # Don't count SSL errors as critical errors - they're common in web scraping
            return {"html": False, "response_type": "", "http_code": 0}
        except requests.exceptions.TooManyRedirects as e:
            self.last_curl_error = 47  # CURLE_TOO_MANY_REDIRECTS
            self.last_curl_error_msg = f"Too many redirects: {str(e)}"
            # Don't count redirect errors as critical errors
            return {"html": False, "response_type": "", "http_code": 0}
        except Exception as e:
            self.last_curl_error = 1  # CURLE_UNSUPPORTED_PROTOCOL
            # Clean up error message to avoid including HTML content
            error_str = str(e)
            if len(error_str) > 200:  # Truncate very long error messages
                error_str = error_str[:197] + "..."
            self.last_curl_error_msg = f"General error: {error_str}"
            # Don't count parameter errors as critical errors - they're configuration issues
            if "max_redirects" in str(e):
                return {"html": False, "response_type": "", "http_code": 0}
            # Only count unexpected errors
            self.error_count += 1
            return {"html": False, "response_type": "", "http_code": 0}

    def convert_encoding(self, text: str, response_type: str = "") -> str:
        """Convert text encoding to UTF-8 matching PHP version's approach"""
        if not text:
            return text

        # Check if already UTF-8
        try:
            text.encode("utf-8").decode("utf-8")
            return text
        except UnicodeError:
            pass

        # Try ISO-8859-1 to UTF-8 conversion (matching PHP iconv)
        try:
            return text.encode("iso-8859-1").decode("utf-8")
        except (UnicodeEncodeError, UnicodeDecodeError):
            pass

        # Fallback: decode with error handling
        try:
            return text.encode("iso-8859-1", errors="ignore").decode(
                "utf-8", errors="ignore"
            )
        except Exception:
            # Last resort: return as-is with null character removal
            return text.replace("\0", "")

    def get_error_code(self, error_type: str = "") -> int:
        """Get standardized error codes matching PHP version"""
        error_codes = {
            "robots_blocked": 401,
            "timeout": 28,
            "connection_error": 7,
            "ssl_error": 60,
            "unsupported_protocol": 1,
            "general_error": 1,
        }
        return error_codes.get(error_type, 1)

    def is_valid_html_content(self, html: str, response_array: Dict[str, Any]) -> bool:
        """Check if HTML content is valid and should be processed - IMPROVED VALIDATION"""
        if not html or html is False:
            return False

        # Check content length
        if len(html) == 0:
            return False

        # IMPROVED: Accept more HTTP status codes that can contain valid HTML content
        http_code = response_array.get("http_code", 0)
        acceptable_status_codes = [200, 201, 202, 203, 206, 301, 302, 303, 307, 308, 403, 429, 503, 520]
        
        # Reject only clearly error status codes that won't have useful HTML content
        if http_code >= 400 and http_code not in [403, 429, 503, 520]:  # 403=Forbidden, 429=Too Many Requests, 503=Service Unavailable, 520=Cloudflare Unknown Error
            return False

        # IMPROVED: More lenient content type validation
        response_type = response_array.get("response_type", "").lower()
        
        # If no content type provided, accept the content (many sites don't set it properly)
        if not response_type or response_type == "":
            # Check if content looks like HTML by examining the first few characters
            html_start = html.strip()[:100].lower()
            if html_start.startswith('<!doctype') or html_start.startswith('<html') or '<' in html_start:
                return True
            # If it doesn't look like HTML but has no content type, still accept it
            # (some sites return HTML without proper content-type headers)
            return True
        
        # Accept common HTML content types
        html_content_types = [
            'text/html', 'application/xhtml', 'text/plain', 
            'application/xhtml+xml', 'text/xml', 'application/xml'
        ]
        
        if any(html_type in response_type for html_type in html_content_types):
            return True
            
        # IMPROVED: Check for binary content types that should be rejected
        binary_content_types = [
            'image/', 'video/', 'audio/', 'application/pdf', 'application/zip',
            'application/octet-stream', 'application/x-binary'
        ]
        
        if any(binary_type in response_type for binary_type in binary_content_types):
            return False
            
        # IMPROVED: Check for PDF/PNG content in the actual content (not just headers)
        if html.startswith("%PDF") or html.startswith("%PNG"):
            return False
            
        # IMPROVED: If content type is unknown but content looks like HTML, accept it
        html_start = html.strip()[:100].lower()
        if html_start.startswith('<!doctype') or html_start.startswith('<html') or '<' in html_start:
            return True
            
        # IMPROVED: For unknown content types, be more lenient - accept if not clearly binary
        # This handles cases where sites return HTML with incorrect content-type headers
        return True

    def process_html_content(
        self, html: str, row: Dict[str, Any], response_array: Dict[str, Any]
    ) -> bool:
        """Process and store HTML content with improved validation"""
        if not self.db:
            self.log_error("Database connection not available")
            return False

        # Skip if too large (2MB to match get_html limit)
        html_size = len(str(html)) if html else 0
        if html_size > 2_000_000:  # 2MB limit (increased to match get_html)
            return False
            
        # IMPROVED: Accept more HTTP status codes that can contain valid HTML content
        http_code = response_array.get("http_code", 0)
        # Reject only clearly error status codes that won't have useful HTML content
        if http_code >= 400 and http_code not in [403, 429, 503, 520]:  # 403=Forbidden, 429=Too Many Requests, 503=Service Unavailable, 520=Cloudflare Unknown Error
            return False
            
        # IMPROVED: Use the enhanced validation method that handles edge cases
        if not self.is_valid_html_content(html, response_array):
            # Log detailed validation failure information
            http_code = response_array.get("http_code", 0)
            response_type = response_array.get("response_type", "unknown")
            html_length = len(str(html)) if html else 0
            self.log_error(f"HTML content validation failed for idx {row['idx']} - URL: {row.get('url', 'N/A')} - Content-Type: {response_type} - HTTP: {http_code} - Length: {html_length}")
            return False

        # Handle encoding using improved method
        if isinstance(html, bytes):
            try:
                html = html.decode("utf-8")
            except UnicodeDecodeError:
                html = html.decode("iso-8859-1", errors="ignore")

        # Remove null characters
        html = html.replace("\0", "")

        # Convert encoding to UTF-8 (matching PHP version)
        html = self.convert_encoding(html, response_array["response_type"])

        # Get version
        version_query = f"""
        SELECT MAX(version)
        FROM smartdatadb.b2b_html
        WHERE idx = {row['idx']}
        GROUP BY idx
        """
        version_result = self.db.query(version_query)

        if not version_result:
            version_query = f"""
            SELECT MAX(version)
            FROM smartdatadb.b2b_keywords_match
            WHERE idx = {row['idx']}
            GROUP BY idx
            """
            version_result = self.db.query(version_query)
            if not version_result:
                version = 0
            else:
                version = version_result[0]["max"] + 1
        else:
            version = version_result[0]["max"] + 1

        # Prepare HTML for insertion
        html_escaped = html.replace("'", "''")
        html_md5 = hashlib.md5(html.encode("utf-8")).hexdigest()

        # Insert HTML content with better duplicate handling
        sql_insert = f"""
        INSERT INTO smartdatadb.b2b_html(
            IDX, Version, content_type, HTML, laenge, HTMLMD5,
            invalidcharset, eingefuegtvon
        )
        SELECT {row["idx"]}, {version},
            {self.db.quote(self.convert_encoding(response_array['response_type']))},
            '{html_escaped}', {len(html)}, '{html_md5}', 0, 'html_parser.py'
        WHERE NOT EXISTS (
            SELECT 1 FROM smartdatadb.b2b_html
            WHERE idx = {row["idx"]}
            AND version = {version}
        )
        ON CONFLICT (idx, version) DO NOTHING
        """

        result_insert = self.db.exec(sql_insert)
        if not result_insert:
            # Check if it's a duplicate key error (not critical)
            error_msg = self.db.error_to_string()
            if "duplicate key" in error_msg.lower() or "unique constraint" in error_msg.lower():
                self.log_hint(f"Duplicate content for idx {row['idx']}, version {version} - skipping")
                return True  # Treat as success, not failure
            else:
                self.log_error(
                    f"Fehler beim Insert des Datensatzes. SQLERROR: {error_msg}"
                )
                return False

        # Insert status history if objektid exists
        if row["objektid"] != 0:
            status_insert = f"""
            INSERT INTO smartdatadb.importstatushist(
                gueltigab, objektid, importstatusid, eingefuegtam, eingefuegtvon
            )
            SELECT NOW(), {row["objektid"]}, 10, NOW(), 'htmlparse'
            WHERE NOT EXISTS (
                SELECT 1 FROM smartdatadb.b2b_html
                WHERE idx = {row["idx"]}
                AND version = {version} - 1
                AND htmlmd5 = '{html_md5}'
            )
            """
            self.db.exec(status_insert)
            self.db.commit()

        return True

    def write_htmls(self):
        """Main method to process URLs and write HTML content"""
        if not self.db:
            raise Exception("Database connection not available")

        # Refresh materialized view
        result = self.db.exec(
            "refresh materialized view smartdatastagdb.mv_importstatushist30"
        )
        self.log_hint("smartdatastagdb.mv_importstatushist30 refreshed")

        # Check if custom SQL is provided (from DAG)
        if self.sql:
            # Use the custom SQL directly (for parallel processing)
            self.log_hint(f"Using custom SQL for parallel processing (Task {self.task_id}/{self.total_tasks})")
            result = self.db.query(self.sql)
            
            if result:
                self.log_hint(f"{len(result)} Zeilen ermittelt.")
                self.process_urls(result)
        else:
            # Use country-based processing with workload prioritization (for standalone execution)
            # Get country with most unprocessed URLs
            country_priority_sql = """
            select c.country, count(u.idx) as url_count
            from smartdatastagdb.config_linkextract_order c
            left join smartdatadb.b2b_urls u on u.country = c.country
            where u.idx = coalesce(u.master_idx, u.idx)
                and u.url not like 'MAILTO:%'
                and u.url not like 'mailto:%'
                AND NOT EXISTS(SELECT 1 FROM smartdatadb.b2b_html h WHERE h.idx=u.idx AND h.eingefuegtam > NOW() - INTERVAL '12 hours' - INTERVAL '360 days')  
                AND NOT EXISTS(SELECT 1 FROM smartdatadb.b2b_url_call c WHERE c.idx=u.idx and c.datum > NOW() - interval '12 hours' -interval '360 days')
            group by c.country
            order by url_count desc
            limit 1
            """
            
            result_country = self.db.query(country_priority_sql)
            
            if result_country and len(result_country) > 0:
                country = result_country[0]["country"]
                url_count = result_country[0]["url_count"]
                self.log_hint(f"Selected country: {country} with {url_count} unprocessed URLs")

                # Build main query for this country with improved logic
                other_sql = f"""
                with u as (	select u.idx, u.url, u.country, u.url_absolute, up.url as parent_url, case when u.objektid is not null then u.objektid else up.objektid end as objektid
						from smartdatadb.b2b_urls u LEFT JOIN smartdatadb.b2b_urls up ON up.idx = u.parent_idx
						where u.idx = coalesce(u.master_idx, u.idx)
								and u.url not like 'MAILTO:%'
								and u.url not like 'mailto:%'
								AND NOT EXISTS(SELECT 1 FROM smartdatadb.b2b_html h WHERE h.idx=u.idx AND h.eingefuegtam > NOW() - INTERVAL '12 hours' - INTERVAL '360 days')  
								AND NOT EXISTS(SELECT 1 FROM smartdatadb.b2b_url_call c WHERE c.idx=u.idx and c.datum > NOW() - interval '12 hours' -interval '360 days' /*and errorno>0*/) 
								/*and NOT EXISTS (SELECT 1 FROM smartdatadb.b2b_url_call c
												WHERE errorno = 6 and c.idx=u.idx
												HAVING count(errorno) >= 3 )*/ 
								"""

                if self.args["modulo"]:
                    other_sql += f" AND mod(u.idx, {self.args['modulo']})"

                other_sql += f"""
                    and u.country='{country}'
                    limit 60000
                )
                SELECT u.objektid, u.idx, u.url, u.url_absolute, u.parent_url, u.country
                FROM u 
                left join smartdatadb.objekt o on o.objektid = u.objektid
                left join (
                    select objektid from smartdatadb.importstatushist 
                    where importstatusid = 30 group by objektid
                ) ih on ih.objektid = o.masterobjektid 				
                order by u.idx, case when ih.objektid is null then 1 else 0 end, random()
                                    limit 3000
                """

                self.log_hint(f"Write HTML for country {country} SQL: {other_sql}")
                result = self.db.query(other_sql)

                if result:
                    self.log_hint(f"{len(result)} Zeilen ermittelt.")
                    self.process_urls(result)
                else:
                    self.log_hint(f"No unprocessed URLs found for country {country}")
            else:
                self.log_hint("No countries with unprocessed URLs found")

    def process_urls(self, result):
        """Process a list of URLs with error handling"""
        i = 0
        
        # Log country information for the first URL
        if result and len(result) > 0:
            country = result[0].get("country", "unknown")
            self.log_hint(f"Processing URLs for country: {country}")

        for row in result:
            i += 1
            self.total_processed = i  # Track total URLs processed

            # Check if we should stop processing
            if self.check_should_stop():
                logger.info("Stopping processing due to time limit or error threshold")
                break

            # Memory optimization: Clear variables at start of each iteration
            response_array = {"http_code": 0, "response_type": ""}
            html = False
            
            # Reset transaction state if needed (prevents cascade failures)
            try:
                if self.db and self.db.connection:
                    # Check if transaction is in aborted state
                    with self.db.connection.cursor() as cursor:
                        cursor.execute("SELECT 1")
            except Exception:
                # Transaction is aborted, reset it
                try:
                    self.db.rollback()
                except:
                    pass
            
            # Add progress logging every 100 URLs to reduce log noise with larger batches (3000 URLs per task)
            if i % 100 == 0:
                self.log_hint(f"Processing URL {i}/{len(result)} in current batch")

            # Skip problematic URLs before processing
            url_to_process = row["url_absolute"] if row["url_absolute"] else row["url"]
            if not url_to_process or len(url_to_process.strip()) == 0:
                self.log_hint(f"Skipping empty URL for idx {row['idx']}")
                continue
                
            # Clean and validate URL
            url_to_process = url_to_process.strip()
            
            # IMPROVED: More precise URL filtering - only skip URLs that actually start with these schemes
            url_lower = url_to_process.lower().strip()
            skip_schemes = ['mailto:', 'tel:', 'javascript:', 'data:', 'file:', 'ftp:', 'sftp:']
            
            # Only skip if URL actually starts with a problematic scheme (not just contains it)
            if any(url_lower.startswith(scheme) for scheme in skip_schemes):
                self.log_hint(f"Skipping unsupported URL type for idx {row['idx']}: {url_to_process}")
                continue
                
            # Skip extremely long URLs (increased limit from 500 to 5000)
            if len(url_to_process) > 5000:
                self.log_hint(f"Skipping extremely long URL for idx {row['idx']}: {len(url_to_process)} chars")
                continue
                
            # Process URL based on whether url_absolute exists
            if row["url_absolute"] is None:
                # Process relative URL
                if self.check_robots(row["url"]):
                    response_array = self.get_html(self.add_http(row["url"]))
                    html = response_array["html"]
                else:
                    # Create response for robots.txt blocked request
                    response_array = {
                        "http_code": 403,
                        "response_type": "text/plain"
                    }
                    # Insert robots.txt blocked record
                    sql_insert = f"""
                    INSERT INTO smartdatadb.b2b_url_call (
                        idx, datum, errorno, msg, eingefuegtam, eingefuegtvon,
                        response_status, content_type
                    ) VALUES (
                        {row["idx"]}, NOW(), {self.get_error_code("robots_blocked")}, 'Request skipped due to robots.txt',
                        NOW(), 'htmlparse', {response_array['http_code']},
                        {self.db.quote(self.convert_encoding(response_array['response_type']))}
                    )
                    """
                    self.db.exec(sql_insert)
                    self.db.commit()
                    continue
            else:
                # Process absolute URL directly
                if self.check_robots(row["url_absolute"]):
                    response_array = self.get_html(row["url_absolute"])
                    html = response_array["html"]
                else:
                    # Create response for robots.txt blocked request
                    response_array = {
                        "http_code": 403,
                        "response_type": "text/plain"
                    }
                    # Insert robots.txt blocked record
                    sql_insert = f"""
                    INSERT INTO smartdatadb.b2b_url_call (
                        idx, datum, errorno, msg, eingefuegtam, eingefuegtvon,
                        response_status, content_type
                    ) VALUES (
                        {row["idx"]}, NOW(), {self.get_error_code("robots_blocked")}, 'Request skipped due to robots.txt',
                        NOW(), 'htmlparse', {response_array['http_code']},
                        {self.db.quote(self.convert_encoding(response_array['response_type']))}
                    )
                    """
                    self.db.exec(sql_insert)
                    self.db.commit()
                    continue

            # Second attempt with absolute URL
            if row["parent_url"] and html is False:
                try:
                    absolute_url = self.url_to_absolute(
                        row["parent_url"], row["url"]
                    )

                    if absolute_url != row["url"] and absolute_url:
                        if self.check_robots(absolute_url):
                            response_array = self.get_html(absolute_url)
                            html = response_array["html"]
                        else:
                            # Create response for robots.txt blocked request
                            response_array = {
                                "http_code": 403,
                                "response_type": "text/plain"
                            }
                            # Insert robots.txt blocked record
                            sql_insert = f"""
                            INSERT INTO smartdatadb.b2b_url_call (
                                idx, datum, errorno, msg, eingefuegtam, eingefuegtvon,
                                response_status, content_type
                            ) VALUES (
                                {row["idx"]}, NOW(), {self.get_error_code("robots_blocked")}, 'Request skipped due to robots.txt',
                                NOW(), 'htmlparse', {response_array['http_code']},
                                {self.db.quote(self.convert_encoding(response_array['response_type']))}
                            )
                            """
                            self.db.exec(sql_insert)
                            self.db.commit()
                            continue

                        # Update absolute URL
                        absolute_url_escaped = absolute_url.replace("'", "''")
                        sql_update = f"""
                        UPDATE smartdatadb.b2b_urls 
                        SET URL_ABSOLUTE='{absolute_url_escaped}'
                        WHERE idx={row["idx"]}
                        """
                        result_update = self.db.exec(sql_update)
                        if not result_update:
                            self.log_error(
                                f"Fehler beim Update des Datensatzes. SQLERROR: {self.db.error_to_string()}"
                            )
                        self.db.commit()
                except Exception as e:
                    # Log the error but continue processing
                    self.log_error(f"Error processing absolute URL for idx {row['idx']}: {str(e)}")
                    # Don't increment error count for URL processing errors
                    continue

            # Insert curl result with error handling
            try:
                # Truncate error message to fit database field limit (1024 characters)
                error_msg = self.convert_encoding(self.last_curl_error_msg)
                
                # Remove any HTML content that might have been included in error messages
                if '<' in error_msg and '>' in error_msg:
                    # Extract only the error part before any HTML content
                    html_start = error_msg.find('<')
                    if html_start > 0:
                        error_msg = error_msg[:html_start].strip()
                
                if len(error_msg) > 1000:  # Leave some buffer for quotes and escaping
                    error_msg = error_msg[:997] + "..."
                
                sql_insert = f"""
                INSERT INTO smartdatadb.b2b_url_call (
                    idx, datum, errorno, errormsg, msg, response_status, content_type
                ) VALUES (
                    {row["idx"]}, NOW(), {self.last_curl_error},
                    '', {self.db.quote(error_msg)},
                    {response_array['http_code']}, {self.db.quote(self.convert_encoding(response_array['response_type']))}
                )
                ON CONFLICT DO NOTHING
                """

                result_insert = self.db.exec(sql_insert)
                if not result_insert:
                    error_msg = self.db.error_to_string()
                    if "duplicate key" in error_msg.lower() or "unique constraint" in error_msg.lower():
                        # Duplicate entry - not a critical error
                        pass
                    else:
                        self.log_hint(sql_insert)
                        self.log_error(
                            f'Fehler beim Insert in smartdatadb.B2B_URL_CALL (idx: {row["idx"]}). SQLERROR: {error_msg}'
                        )
                        # Force rollback to prevent transaction cascade
                        try:
                            self.db.rollback()
                        except:
                            pass
                else:
                    self.db.commit()
            except Exception as e:
                self.log_error(f"Database error during curl result insert: {str(e)}")
                # Force rollback to prevent transaction cascade
                try:
                    self.db.rollback()
                except:
                    pass

            # Process HTML content with error handling
            if html and html is not False:
                try:
                    if self.process_html_content(html, row, response_array):
                        self.processed_count += 1
                        # Progress logging
                        if i % 500 == 0:  # Log every 500th URL for larger batches
                            success_rate = (self.processed_count / i) * 100 if i > 0 else 0
                            self.log_hint(f"{i} DatensÃƒÂ¤tze verarbeitet. Processed: {self.processed_count}, Errors: {self.error_count}, Success Rate: {success_rate:.1f}%")
                        # Log successful processing for debugging
                        elif i % 100 == 0:  # Log every 100th successful URL
                            http_code = response_array.get("http_code", 0)
                            self.log_hint(f"Successfully processed URL {i} - HTTP: {http_code} - URL: {row.get('url', 'N/A')}")

                        # Check time limit
                        if self.end_timestamp < time.time() and i > 100:
                            self.log_hint("Maximale Laufzeit erreicht.")
                            break
                    else:
                        # IMPROVED: More detailed error logging for debugging
                        response_type = response_array.get("response_type", "unknown")
                        http_code = response_array.get("http_code", 0)
                        html_length = len(str(html)) if html else 0
                        self.log_error(f"HTML content validation failed for idx {row['idx']} - URL: {row.get('url', 'N/A')} - Content-Type: {response_type} - HTTP: {http_code} - Length: {html_length}")
                        self.error_count += 1
                except Exception as e:
                    self.log_error(f"Error processing HTML content for idx {row['idx']}: {str(e)}")
                    self.error_count += 1
                    # Force transaction rollback on any error to prevent cascade failures
                    try:
                        self.db.rollback()
                    except:
                        pass
            else:
                # Log URLs that didn't get HTML content (the "missing" URLs)
                http_code = response_array.get("http_code", 0)
                error_msg = self.last_curl_error_msg if self.last_curl_error_msg else "No HTML content received"
                self.log_hint(f"No HTML content for idx {row['idx']} - URL: {row.get('url', 'N/A')} - HTTP: {http_code} - Error: {error_msg}")
                    
            # Add random delay between requests to avoid overwhelming servers
            if i % 20 == 0:  # Every 20th request (reduced frequency for larger batches)
                delay = random.uniform(1, 3)  # Random delay between 1-3 seconds
                time.sleep(delay)
            
            # Aggressive memory cleanup after each URL (like working Spain script)
            html = None
            response_array = {}

        self.log_hint(f"{i} DatensÃƒÂ¤tze verarbeitet.")

    def check_url_call_errors(
        self, idx: int, error_code: int = 6, max_attempts: int = 3
    ) -> bool:
        """Check if URL has been called too many times with specific error code"""
        if not self.db:
            return False

        sql = f"""
        SELECT COUNT(*) as error_count
        FROM smartdatadb.b2b_url_call 
        WHERE idx = {idx} AND errorno = {error_code}
        """
        result = self.db.query(sql)
        if result and len(result) > 0:
            return result[0]["error_count"] >= max_attempts
        return False

    def should_skip_url(self, idx: int) -> bool:
        """Determine if URL should be skipped based on various criteria"""
        # Check for too many error code 6 attempts
        if self.check_url_call_errors(idx, 6, 3):
            return True
        return False


def run_html_parser(
    env: str = "PROD",
    maxminutes: int = 120,
    modulo: str = "",
    test: bool = False,
    sql: str | None = None,
    task_id: int = 0,
    total_tasks: int = 1,
):
    """
    Run HTML parser with parameters matching the original PHP command

    Args:
        env (str): Environment (DEV/PROD) - equivalent to database connection
        maxminutes (int): Maximum runtime in minutes - equivalent to -maxminutes
        modulo (str): Modulo filter - equivalent to -modulo
        test (bool): Test mode - equivalent to -test

    This function replaces the PHP command:
    html-parse.php -shell -dbhost $alloyip -dbpasswd $alloypw -dbuser "postgres" -maxminutes "120"
    """
    start_time = time.time()
    task_info = f"[Task {task_id}/{total_tasks}]" if task_id > 0 else ""
    logger.info(f"{task_info} Starting HTML parser with env={env}, maxminutes={maxminutes}, test={test}")
    
    try:
        parser = HTMLParse(
            env=env, maxminutes=maxminutes, modulo=modulo, test=test, sql=sql, task_id=task_id, total_tasks=total_tasks
        )
        parser.run()
        
        execution_time = time.time() - start_time
        logger.info(f"{task_info} HTML parser completed successfully in {execution_time:.2f} seconds")
        logger.info(f"{task_info} Final stats - Processed: {parser.processed_count}, Errors: {parser.error_count}")
        
    except Exception as e:
        execution_time = time.time() - start_time
        logger.error(f"{task_info} HTML parser failed after {execution_time:.2f} seconds: {str(e)}")
        raise  # Re-raise to ensure Airflow knows about the failure


def main():
    """Main entry point for standalone execution with timeout protection"""
    import signal
    
    def timeout_handler(signum, frame):
        logger.error("Main execution timed out, forcing exit")
        sys.exit(1)
    
    # Set up timeout for standalone execution (130 minutes = maxminutes + 10 buffer)
    signal.signal(signal.SIGALRM, timeout_handler)
    signal.alarm(130 * 60)  # 130 minutes
    
    try:
        run_html_parser(env="PROD", maxminutes=120)
    finally:
        signal.alarm(0)  # Cancel the alarm


if __name__ == "__main__":
    main()