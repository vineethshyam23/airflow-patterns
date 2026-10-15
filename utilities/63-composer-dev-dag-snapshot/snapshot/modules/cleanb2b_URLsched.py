#!/usr/bin/env python3

#   Scheduler Basisklasse
import re
from urllib.parse import urlparse
import logging
import psycopg2
from typing import Optional
from .db_connections import initialize_db_connection

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class Extractor:

    conn: Optional[psycopg2.extensions.connection] = None
    doc = None
    domains = set()
    endpoints = set()
    url_idx = {}
    urls = []
    urldict = {}
    idxset = set()
    wwwdict = {}
    schemadict = {}

    argmap = {
        "shell": "Shell",
        "anzahl": "Anzahl",
        "user": "User",
        "pw": "PW",
        "host": "Host",
        "port": "Port",
        "db": "DB",
    }

    def __init__(self, conn_id: str = "google_alloydb_dev", limit: int = 50000):
        self.limit = limit
        self.conn_id = conn_id
        self.conn = initialize_db_connection()

    def insert_url(self, urllist):
        if self.conn is None:
            raise ValueError("Database connection not initialized")
        query = """
        UPDATE smartdatadb.b2b_urls SET
            url_clean = %s,
            geaendertam = NOW(),
            geaendertvon = 'urlcleaner'
        WHERE idx = %s
        ;
        """
        with self.conn.cursor() as cur:
            for url in urllist:
                try:
                    cur.execute(query, url)
                except Exception as e:
                    logging.error(str(e))
            self.conn.commit()

    def fetch_urls(self):
        if self.conn is None:
            raise ValueError("Database connection not initialized")
        query = """
        SELECT idx,
            CASE
                WHEN url_absolute IS NULL THEN url
                ELSE url_absolute
            END AS url
        FROM smartdatadb.b2b_urls
        WHERE url_clean IS NULL
        LIMIT %s
        """
        with self.conn.cursor() as cur:
            cur.execute(query, (self.limit,))
            return cur.fetchall()

    def process_urls(self):
        """
        Iterates over all urls's found in b2b_urls
        splitting each in their parts (netloc, path)
        and simplifies the parts to make them
        unambigious
        """
        logging.info("Starting process_urls")
        wwwre = re.compile(".*www\.")
        docs = self.fetch_urls()
        if not docs:
            logging.info("No URLs found to process")
            return

        paths, params, queries = 0, 0, 0
        toplevels = [
            re.compile(".*\.de"),
            re.compile(".*\.com"),
            re.compile(".*\.pl"),
            re.compile(".*\.at"),
            re.compile(".*\.info"),
            re.compile(".*\.it"),
            re.compile(".*\.eu"),
            re.compile(".*\.berlin"),
            re.compile(".*\.bar"),
            re.compile(".*\.menu"),
            re.compile(".*\.site"),
            re.compile(".*\.pt"),
            re.compile(".*\.net"),
        ]
        urllist = []

        for doc in docs:
            logging.info("process_urls while loop")
            if doc:
                idx, url = doc
                if url:
                    parts = urlparse(url.lower())
                    domain = parts.netloc
                    paths = paths + 1
                    path = parts.path
                    www = url.find("www.") >= 0 and "www." or ""
                    schema = "http://" if url.find("http://") >= 0 else "https://"
                    if not domain:
                        for toplevel in toplevels:
                            if toplevel.findall(path):
                                domain = toplevel.findall(path)[0]
                                path = toplevel.sub("", path)
                    domain = wwwre.sub("", domain)
                    logging.info("process_urls domain: " + domain)
                    if parts.path:
                        if path.endswith("index.html"):
                            path = path.replace("/index.html", "")
                        elif path.endswith("index.htm"):
                            path = path.replace("/index.htm", "")
                        if path.endswith(".html"):
                            path = path.replace(".html", "")
                        elif path.endswith(".htm"):
                            path = path.replace(".htm", "")
                        # if path.endswith('/'):
                        path = path.rstrip("/")
                        if path:
                            path = path.startswith("/") and path or "/" + path
                            path = (
                                path.startswith("/www.")
                                and path.replace("/www.", "")
                                or path
                            )

                    else:
                        path = ""
                    urltuple = tuple([domain + path, idx])
                    urllist.append(urltuple)
                    print(urltuple)

        self.insert_url(urllist)
        logging.info("Clean B2B-URL completed")


# if __name__ == "__main__":
#     e = Extractor()
#     e.process_urls()
