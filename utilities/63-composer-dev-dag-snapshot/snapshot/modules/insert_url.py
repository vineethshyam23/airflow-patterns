#from console import Console
#from .url import url as modul_url 

import json
import traceback
import sys
import logging
from urllib.parse import parse_qsl, quote_plus, urlparse,urljoin
from timeout_decorator import timeout, TimeoutError as TD_timeoutError
from typing import Optional

import modules.url as modul_url #need to import as entire module
from .db_connections import initialize_db_connection

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

class InsertUrl(): 


    # def __init__(self,proc) -> None:
    #     super().__init__()
    #     self.proc = proc

    def __init__(self,conn_id: str = "google_alloydb_dev", limit: int = 50000,proc=None):
        super().__init__()
        self.proc = proc
        self.limit = limit
        self.conn_id = conn_id
        self.conn = initialize_db_connection()


    def next_doc(self):

        """
        Common method to iterate over all HTML-Documents
        to be processed
        """
       
        try:
            self.doc = next(self.docs_to_process)
        except:
            print("No docs to process")
            raise Exception

   
    def _check_url(self, url): 

        url_blacklist = ['@','.facebook','login','registration','javascript','tel:','reservieren','reservierung','melden','reserver','mailto','api.proxycrawl']
        insert_flag = 1

        if url:             
            for word in url_blacklist:
                if url.lower().find(word) != -1:

                    insert_flag = 0
                    break            
        else: 
            insert_flag = 0

        return insert_flag
    
    def _convert_data_to_import(self, data):

        if data.find("'") != -1:

            data=data.replace("'","''")
        return data

    @timeout(40)
    def _creat_url_fingerprint(self, url):
    
        urlitem = modul_url.gen_urlitem(url)
        urlitem= json.loads(modul_url.ujson.dumps(urlitem))
       
        url_www = urlitem['data']['www']
        url_domain = urlitem['data']['domain']
        url_path = urlitem['data']['path']
        url_query = urlitem['data']['query']
        url_scheme = urlitem['data']['scheme']
        url_fingerprint = urlitem['data']['fingerprint']

        return self._convert_data_to_import(url_fingerprint),url_scheme,self._convert_data_to_import(url_domain),url_www,self._convert_data_to_import(url_path),self._convert_data_to_import(url_query)
    
    
    def _call_import_proc(self,proc,idx, url_fingerprint, url, eingefuegtvon, url_scheme, url_domain, url_www, url_path, country, objektid, url_query,parent_idx):
        
        Query = f"call {proc}({idx},'{url_fingerprint}','{url}','{eingefuegtvon}','{url_scheme}','{url_domain}','{url_www}','{url_path}','{country}',{objektid},'{url_query}',{parent_idx})"

        #with self.connection.cursor() as curs:           
        with self.conn.cursor() as curs:
            curs.execute(Query)

            try:
                #self.connection.commit()
                self.conn.commit()
               
            except:
                #self.connection.rollback()
                self.conn.rollback()
                self.handel_exception(1)

    def _url_update(self, url,url_parent):

        if url and len(url)<= 512:
                                 
            if ((url.startswith('./') or url.startswith('/') ) and url_parent != url.strip()) or \
                ((not url.strip().startswith('http')) and (not url.strip().startswith('www')) and url_parent != url.strip()) : #here, the URLs that do not have the same parentidx are covered if 1) they start with '/' or './', or 2) they do not start with 'http/www'
                                                           
                url = urljoin(url_parent,url)
            
        return url


    @staticmethod
    def handel_exception(exitcode=1):

        exc_type, exc_value, exc_traceback = sys.exc_info()
        print("*** print_tb:")
        traceback.print_tb(exc_traceback, file=sys.stderr)
        print("*** print_exception:")# exc_type below is ignored on 3.5 and later
        traceback.print_exception(exc_type, exc_value, exc_traceback,file=sys.stderr)   
        exit(exitcode)
