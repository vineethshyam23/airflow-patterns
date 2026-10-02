#!/usr/bin/env python3

import psycopg2
import sys
from psycopg2.extras import execute_values
from typing import List, Tuple

class PostgresDB:
    ''' Class t oprovide a connection to the Postgres Databse

    '''

    connection = None
    db_version = None
    _lastError = None
    connected = False
    closed = None

    def __init__(self, provided_connection=None,user="REDACTED", password="REDACTED", host="REDACTED", port="", database=""):
        """ Init PostgresqlDB class. An existing connection can be provided
        as an argument (provided_connection). If no connection is provided, the
        connection is established with the parameters"""

        if provided_connection:
            self.connection = provided_connection
        else:   #prepare config datei 
             
            self.connect(user, password, host, port, database)

        self._get_db_version()
        if self.db_version:
            self.connected, self.closed = True, False
        else:
            raise Exception("No database connection established")


    def connect(self, user, password, host, port, database):
        """Connect method that can be called by __init__"""
        try:
            self.connection = psycopg2.connect(
                user=user, password=password, host=host, port=port, database=database
            )

        except (Exception) as error:
            self._lastError = error
            print("Error while connecting to PostgreSQL", error)


    def close(self):
        """Close the database connection"""
        if self.connected:
            self.connection.close()
        self.connected, self.closed = False, True


    def _get_db_version(self):
        """Sets the database version at self.db_version"""
        with self.connection.cursor() as curs:
            curs.execute("SELECT version();")
            self.db_version = curs.fetchone()[0]


    def ping(self):
        """Executes a query that should work in any case if
        the database connection is established. Returns Pong"""
        try:
            with self.connection.cursor() as curs:
                curs.execute("SELECT 'Pong!';")
                print(curs.fetchone()[0])
        except Exception as error:
            self._lastError = error
            print("The following Error occoured:\n" + str(self._lastError))


    def execute(self, sql, itemtuple=None):
        """Executes an arbitary query. If itemtuple is provided, the query is
        executed with the itemtuple as parameters. Returns 0 if successfully
        commited, otherwise the error.
        """
        try:
            with self.connection.cursor() as curs:
                if itemtuple: 
                    curs.execute(sql, itemtuple) 
                else:
                    curs.execute(sql)

            self.connection.commit()
            return 0
        except Exception as e:
            self.connection.rollback()
            return e

    
    def multi_insert(self, sql: List[str], value_template: List[str], itemtuplelist: List[List[Tuple]] = None):
        """
        Executes multiple arbitrary queries to insert multiple values in a performant manner.
        Each query is executed with its corresponding itemtuplelist as parameters, where
        each itemtuplelist is a list of tuples, one tuple for each row to be inserted.

        Parameters:
        - sql: List of SQL query strings.
        - value_template: List of value templates for each SQL query.
        - itemtuplelist: List of item tuples lists, one for each query.

        Returns:
        - 0 on success, or raises an exception on failure.
        """
        # Check if all parameters are lists
        all_lists = all(isinstance(param, list) for param in [sql, value_template, itemtuplelist if itemtuplelist is not None else []])
        # Check if all parameters are strings (itemtuplelist is ignored in this check as it's supposed to be a list of lists)
        all_strings = all(isinstance(param, str) for param in [sql, value_template] if param is not None)

        if all_lists:
            pass
        elif all_strings:
            # Convert strings to lists of length 1
            sql = [sql]
            value_template = [value_template]
            itemtuplelist = [itemtuplelist] if itemtuplelist is not None else None
        else:
            # Mixed case, raise an error
            raise ValueError("Mixed types for parameters; all must be either lists or strings.")

        curs = self.connection.cursor()

        try:
            for i, query in enumerate(sql):
                execute_values(curs, query, itemtuplelist[i], value_template[i])
            self.connection.commit()
            return 0
        except Exception as e:
            self.connection.rollback()
            return e


    def fetch(self, sql, returnfunction=None, itemtuple=None):
        """Fetches and returns the results of a dbquery in an iterative way. 
        Takes a sql query, an optional returnfunction and 
        an optional tuple as arguments.
        Returns the results of the query as a generator object, using the
        returnfunction to process the results if provided."""
        with self.connection.cursor() as curs:
            if itemtuple: 
                curs.execute(sql, itemtuple) 
            else:
                curs.execute(sql)
            
            row = curs.fetchone()
            while row is not None:
                if returnfunction: 
                    yield from returnfunction(row)
                else: 
                    yield row
                row = curs.fetchone()


    def fetch_all(self, sql, returnfunction=None, itemtuple=None): 
        """Fetches and returns the results of a dbquery in an iterative way. 
        Takes a sql query, an optional returnfunction and 
        an optional tuple as arguments.
        Returns the results of the query either directly 
        or using the returnfunction to process the results if provided.
        """
        if not returnfunction: 
            returnfunction=lambda result: result
        with self.connection.cursor() as curs:
            if itemtuple: 
                curs.execute(sql, itemtuple) 
            else:
                curs.execute(sql)
                
            if returnfunction: 
                return returnfunction(curs.fetchall())
            
            return curs.fetchall()


    def fetch_documents(self):
        '''Fetches documents from the table b2b_html'''
        #if not offset.__class__ == int:
        #    return
        query = '''
        SELECT h.idx,
            h.version,
            u.url,
            u.url_absolute,
            h.html,
            u.country
        FROM smartdatadb.b2b_html h
        INNER JOIN smartdatadb.b2b_urls u
            ON u.idx = h.idx
        WHERE NOT EXISTS (
            SELECT 1
            FROM smartdatadb.b2b_keywords_match km
            WHERE km.idx = u.idx
                AND km.version = h.version
        )
        ORDER BY u.idx DESC
            LIMIT  10000'''
        with self.connection.cursor() as curs:
            curs.execute(query)
            for doc in curs:
                yield doc

    def count_documents(self):
        '''Counts downloaded html documents'''
        query = '''
        SELECT COUNT(*) AS cnt
        FROM smartdatadb.b2b_html h
        '''
        with self.connection.cursor() as curs:
            curs.execute(query)
            return [k[0] for k in curs][0]


    def fetch_keywords(self):
        '''Fetch keywords from the table b2b_keywords'''
        query = '''
        SELECT keyword
        FROM smartdatadb.b2b_keywords
        WHERE followlink = 'N'
        '''
        with self.connection.cursor() as curs:
            curs.execute(query)
            return [k[0] for k in curs]


    def fetch_link_keywords(self):
        '''Fetch keywords from th table b2b_keywords which are set to follow links'''
        query = '''
        SELECT keyword
        FROM smartdatadb.b2b_keywords
        WHERE followlink = 'J'
        '''
        with self.connection.cursor() as curs:
            curs.execute(query)
            return [k[0] for k in curs]


    def insert_url(self, url, eingefuegtvon, parentid, country=None, from_keyword=None):
        '''Inserts a single URL in the table b2b_urls'''
        query = '''
        INSERT INTO smartdatadb.b2b_urls (
            idx,
            url,
            eingefuegtam,
            eingefuegtvon,
            parent_idx,
            country,
            from_keyword
        ) VALUES (
            nextval('smartdatadb.b2b_urls_idx'),
            %s, NOW(), %s, %s, %s, %s
        )
        '''
        with self.connection.cursor() as curs:
            curs.execute(query, (
                url,
                eingefuegtvon,
                parentid,
                country,
                from_keyword
            ))


    def insert_keyword_match(self, keyword, position, idx, version, eingefuegtvon, cnt):
        '''Inserts a keyword match for a HTML document'''
        query = '''
        INSERT INTO smartdatadb.b2b_keywords_match (
            keyword,
            pos,
            idx,
            version,
            eingefuegtam,
            eingefuegtvon,
            cnt
        ) VALUES (
            %s, %s, %s, %s, NOW(), %s, %s
        )
        '''
        with self.connection.cursor() as curs:
            curs.execute(query, (
                keyword,
                position,
                idx,
                version,
                eingefuegtvon,
                cnt
            ))


    def __repr__(self):
        '''Reprensentation function (displays database version)'''
        #return str(self.db_version)
        methods = [method for method in dir(self) if callable(getattr(self, method)) and not method.startswith("__")]
        methods_str = "\n ".join(methods)
        return str(self.db_version) + '\nDB methods: \n ' + methods_str
    

def testConnect(pgpassfile='.pgpass'):
    with open(pgpassfile) as f:
        user, password, host, port, database = tuple(f.read().strip().split(':'))
    return PostgresDB(user=user, password=password, host=host, port=port, database=database)

# Decorator to check if the database operation was successful for execute statements optionally
def check_execution_result(func):
    def wrapper(*args, **kwargs):
        db_result = func(*args, **kwargs)
        if db_result:
            print(f"DB operation failed:\n {db_result} \nPlease check the query definitions and make sure the database is available.")
            sys.exit(1) 
        return db_result
    return wrapper