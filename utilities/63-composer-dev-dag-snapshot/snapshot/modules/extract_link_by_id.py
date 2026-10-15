#!/usr/bin/env python3

"""
- Extract links from HTML content
- Process and clean URLs
- Create URL fingerprints and check if the URL is already in the database
- Filter out unwanted URLs (blacklist)
- Convert relative URLs to absolute URLs
- Insert absolute URL into the database
- Insert the keyword matches into the database
"""

import re
import sys
import logging
#from logging import exception
from selectolax.parser import HTMLParser
#from console import Console
from timeout_decorator import timeout, TimeoutError as TD_timeoutError
from .me_extractor import Extractor 
from .insert_url import InsertUrl
from .db_connections import initialize_db_connection

#no use observed in code
import json
import psycopg2 
# import ujson
# from w3lib.url import canonicalize_url
# import hashlib
# import traceback
# from argparse import ArgumentParser 
# from urllib.parse import parse_qsl, quote_plus, urlparse,urljoin
# from typing import Optional

success_message=''
error_message=''

class LinkExtractor(InsertUrl):
   
    argmap = {
        "shell": "Shell",
        "user": "User",
        "pw": "PW",
        "host": "Host",
        "port":"Port",
        "db":"DB"
    }
    
    def __init__(self,conn_id: str = "google_alloydb_dev", limit: int = 1000, proc='smartdatastagdb.insert_url_idxmapping',url_table='smartdatadb.b2b_urls', keywordsmatching_table='smartdatadb.b2b_keywords_match',keywords_table='smartdatadb.b2b_keywords'):
            #super().__init__(proc)
            #self.e = Extractor(console=self, provided_db_connection=self.connection) 
            super().__init__(conn_id, limit,proc)    
            self.url_table = url_table
            self.keywordsmatching_table = keywordsmatching_table
            self.keywords_table = keywords_table
            self.limit = limit
            self.conn_id = conn_id
            self.conn = initialize_db_connection()  
            self.e = Extractor(provided_db_connection=self.conn)                 

    def next_doc(self):
        """
        Common method to iterate over all HTML-Documents
        to be processed
        """
        try:
            # if self.docs_to_process is None:
            #     #logging.info(f"No more html documents to process")
            #     return
            doc = next(self.docs_to_process)
            self.e.doc = {
                "idx": doc[0],
                "version": doc[1],
                "url": doc[2],
                "url_absolute": doc[3],
                "html": doc[4],
                "doc": HTMLParser(doc[4]),
                "country": doc[5],
                "objektid": doc[6]
            }
        except StopIteration:
            #logging.info("No more documents to process")
            self.e.doc = None
        except Exception as err:
            logging.error('next_doc Error: ' + str(err))
            # Re-raise the exception to fail the DAG on critical errors like SQL syntax errors
            raise


    def fetch_keywords(self):
        '''Fetch keywords from the table b2b_keywords'''
        query = f'''
        SELECT keyword
        FROM {self.keywords_table} 
        WHERE followlink = 'N'
        '''
        with self.conn.cursor() as curs:
            curs.execute(query)
            return [k[0] for k in curs]

    def fetch_link_keywords(self):
        '''Fetch keywords from th table b2b_keywords which are set to follow links'''
        query = f'''
        SELECT keyword
        FROM {self.keywords_table} 
        WHERE followlink = 'J'        
        '''
        with self.conn.cursor() as curs:
            curs.execute(query)
            return [k[0] for k in curs]

    def fetch_parent_url(self,idx):
        
        query = '''
        SELECT url
        FROM smartdatadb.b2b_urls
        WHERE idx = %s
        '''  
        with self.conn.cursor() as curs:
            curs.execute(query,(idx,))
            result = curs.fetchall()[0][0]
            url_parent = result.strip() 

            if not url_parent.startswith('http'):
                url_parent = 'http://' + url_parent
           
            return url_parent

    def fetch_parent_domain(self,idx):
        
        query = '''
        SELECT domain
        FROM smartdatadb.idxmapping
        WHERE idx = %s
        '''
        
        with self.conn.cursor() as curs:
            try:
                curs.execute(query,(idx,))
                result = curs.fetchall()[0][0]
          
                return result   
            except:
                return ''
  
    def insert_url_idxmapping(self, fingerprint, url, parent_idx, scheme, www, domain, path, query,country, from_keyword, objektid):
      
           
        with self.conn.cursor() as curs:

            try:     
                query = f"call {self.proc}('{fingerprint}','{url}',{parent_idx},'{scheme}','{domain}','{www}','{path}','{country}','{from_keyword}',{objektid},'{query}')"
                curs.execute(query)
                self.conn.commit()
                  
            except:
                self.conn.rollback()
                logging.error(f"{self.proc}"+' Procedure did not run: '+ query)
                pass      
 
             
    def insert_keyword_match(self, keyword, position, idx, version, eingefuegtvon, cnt):
        '''Inserts a keyword match for a HTML document'''
      
        query = f"""INSERT INTO {self.keywordsmatching_table} (keyword, pos, idx, version, eingefuegtvon, cnt) 
                VALUES (%s,%s,%s,%s,%s,%s) 
                on conflict (idx, version, keyword) do nothing
                ;"""
        
        
        with self.conn.cursor() as curs:
            try:
                curs.execute(query, (
                    keyword,
                    position,
                    idx,
                    version,
                    eingefuegtvon,
                    cnt
                ))
            except Exception as err:
                print(str(err))
                pass

    def fetch_html_documents(self, query,idlist=None):

        '''Fetches documents from the table b2b_html
        '''
        logging.info(f"Fetching html documents from the table b2b_html")
        with self.conn.cursor() as curs:
            curs.execute(query)
            logging.info(f"Fetched {curs.rowcount} html documents from the table b2b_html")
            for doc in curs:
                #logging.info(f"Fetched html document {doc}")
                yield doc  # Returns one document at a time

    #method orchestrates the entire process of extracting, matching, and recording links and keywords from HTML documents in your database,
    def run(self, query):
       
        link_keywords = self.fetch_link_keywords()  
        keywords = self.fetch_keywords() 
        link_keywords = [(kw, re.compile(kw)) for kw in link_keywords]
        keywords = [(kw, re.compile(kw)) for kw in keywords]
        all_keywords = link_keywords + keywords
        self.docs_to_process = self.fetch_html_documents(query) 
        self.next_doc()      
        processed_cnt=1
        
        while True:
            #for each HTML document
            if self.e.doc:   
                self.e.clean_doc()
                self.e.doc['html'] = self.e.doc['doc'].html.lower()
                self.e.extract_links()
                #logging.info(f"Extracted {len(self.e.doc['links'])} links from the HTML document")

                #for each link in the HTML document
                if self.e.doc['links']:
                    for link in self.e.doc['links']:  
                        for keyword in link_keywords:
                            if link[1] and len(link[1])<128: #tagname of link check, check if it contains the keywords
                                
                                if keyword[1].search(str(link[0]).lower() + str(link[1]).lower()):
                                    pos = link[1].find('#')
                                  
                                    if pos >= 0:
                                        url = link[1][pos+1:]
                                    else:
                                        url = link[1]
                                    if url:               
                                       
                                        try:                                                
                                            url_parent_new = self.fetch_parent_url(idx = self.e.doc['idx'] )
                                            url = self._url_update(url,url_parent_new)                                          
                                            url_domain_parentidx = self.fetch_parent_domain(idx = self.e.doc['idx'])
                                            insert_flag = self._check_url(url)                                        
                                            url_fingerprint,url_scheme,url_domain,url_www,url_path,url_query= self._creat_url_fingerprint(url)
                                            if insert_flag ==1 and url_domain == url_domain_parentidx: # domain should be the same:                                                                                                                        
                                                self.insert_url_idxmapping(fingerprint=url_fingerprint,url=self._convert_data_to_import(url),parent_idx=self.e.doc['idx'],country = self.e.doc['country'], from_keyword=keyword[0],objektid=self.e.doc['objektid'], scheme=url_scheme,www=url_www,domain=url_domain, path=url_path, query = url_query)
                                                #logging.info(f"Inserted url idxmapping for url: {url}")
                                                break
                                                
                                        except (TD_timeoutError, ValueError, KeyError, TypeError) as te:
                                            # Skip invalid URLs (e.g., IPv6 URLs that can't be parsed) instead of crashing
                                            logging.warning(f'Skipping invalid URL at idx {self.e.doc["idx"]}: {str(te)}')
                                            continue

                                        except Exception as err:                                        
                                            # Log error but continue processing other links instead of crashing
                                            logging.error(f'Error processing URL at idx {self.e.doc["idx"]}: {str(err)}')
                                            continue
                                            
                #Iterates through all keywords (both regular keywords and link keywords)
                #Counts occurrences of each keyword in the HTML document using regex
                for keyword in all_keywords:
                    cnt = len(keyword[1].findall(self.e.doc['html']))
                    #Inserts keyword matches into the database if the keyword is found in the HTML document
                    if cnt > 0:
                        matches = keyword[1].search(self.e.doc['html'])
                        try:
                            self.insert_keyword_match(
                                keyword=keyword[0],
                                position=matches.span()[0],
                                idx=self.e.doc['idx'],
                                version=self.e.doc['version'],
                                eingefuegtvon='keywordmatch',
                                cnt=cnt
                            )
                            self.conn.commit()
                            #logging.info(f'Keyword match inserted successfully for keyword: {keyword[0]}')

                        except Exception as err:
                            logging.error('Error at inserting keyword match. Error: ' + str(err))
                            self.conn.rollback()
                            continue

                    #If no keyword is found in the HTML document, insert a keyword match in the database with position 0 and count 0
                    else:
                        try:
                            self.insert_keyword_match(
                                keyword=keyword[0],
                                position=0,
                                idx=self.e.doc['idx'],
                                version=self.e.doc['version'],
                                eingefuegtvon='keywordmatch',
                                cnt=0
                            )
                            self.conn.commit()
                            #logging.info(f"Inserted keyword match for keyword: {keyword[0]}")

                        except Exception as err:
                            logging.error('Error at inserting keyword match. Error: ' + str(err))
                            #Error Handling for Not Found Keywords do nothing
                            self.conn.rollback()
                            pass
                                       
            try:
                # Check if we have a document to process
                if self.e.doc is None:
                    logging.info("No more documents to process.")
                    break
                processed_cnt+=1
                self.next_doc()
            except StopIteration:
                # Normal end of iteration
                break
            except Exception as err:
                # Re-raise exceptions to fail the DAG
                logging.error(f'Error in run loop: {str(err)}')
                raise   

        logging.info(f"Number of HTML documents processed with link extraction by ID script are: " +str(int(processed_cnt)-1))

#if __name__ == '__main__':
#def main_function(total_buckets,curr_bucket_num):
def main_function():
   # if len(sys.argv) >= 2:       
    try:
        logging.info("Link Extraction by ID started")
        le = LinkExtractor()    
        #The query selects latest HTML version for each URL (by idx - one per URL) from your database that have not yet been processed for keyword matching. 
        #It returns the document’s index, version, URL, HTML content, country, and object ID, allowing you to identify and process only new or updated documents that still need keyword extraction or analysis.
        query= f""" SELECT h.idx,
                            h.version,
                            u.url,
                            u.url_absolute,
                            h.html,
                            u.country,
                            u.objektid
                        FROM (
                            SELECT *,
                                row_number() OVER (PARTITION BY idx ORDER BY version DESC) AS cnt
                            FROM smartdatadb.b2b_html
                            WHERE idx > 0
                            AND laenge < 32 * 1024 * 1024
                        ) h
                        JOIN smartdatadb.b2b_urls u ON u.idx = h.idx
                        WHERE h.cnt = 1
                        AND NOT EXISTS (
                            SELECT 1
                            FROM smartdatadb.b2b_keywords_match AS ckm
                            WHERE ckm.version = h.version
                                AND ckm.idx = u.idx
                        )
                        AND u.idx > 0
                        ORDER BY h.idx
                    LIMIT {le.limit} ;
                """
        #logging.info(f"Query: {query}")
        #  args = url.parse_args()
        #  le.run(sys.argv[1:])
        le.run(query)       
        #logging.info("LinkExtractor beendet")  
        success_message = 'Link Extraction by ID completed Successfully'
        logging.info(success_message)
        le.conn.close()    
    except Exception as err:
        error_message = 'Link Extraction by ID Failed at Fetching html documents. Error: ' + str(err)
        logging.error(error_message)
        # Re-raise the exception to fail the DAG
        raise
    finally:
        if 'le' in locals():
            le.conn.close()
    

# else:
#     exit(6)
# laenge<32*1024*1024 : (length) is less than 32 megabytes.
