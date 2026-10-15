import psycopg2
import os 
import re
import logging
from .db_connections import initialize_db_connection
from .me_extractor import Extractor 

class MasterKill:

    argmap = {
        "shell": "Shell",
		"user": "User",
        "pw": "PW",
        "host": "Host",
        "port":"Port",
        "db":"DB"
    }                   

    def __init__(self,conn_id: str = "google_alloydb_dev"):
        self.conn_id = conn_id
        self.conn = initialize_db_connection()  
        self.e = Extractor(provided_db_connection=self.conn) 


    def _exec_procedure(self):
        connection = self.conn
        connection.autocommit=True
        cur = connection.cursor()
        cur.execute("call smartdatadb.kill_master_by_regex()")    
        logging.info("Procedure smartdatadb.kill_master_by_regex() successful ")

        cur.close()
        connection.close()

    def run(self):
        logging.info('Start Kill Master..')
        try:
            self._exec_procedure()
            success_message = 'The stored procedure smartdatadb.kill_master_by_regex() is Successfully completed'
            logging.info(success_message)
            return success_message

        except Exception as e:
            error_message = f'The stored procedure smartdatadb.kill_master_by_regex() is Failed: {str(e)}'
            logging.error(error_message)
            return error_message
        finally:
            logging.info('Finish Kill Master..')  

# if __name__ == '__main__':
def main_function():
    d = MasterKill()
    return d.run()
