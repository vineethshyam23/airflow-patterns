from airflow.operators.python import PythonOperator
import pandas as pd
from sqlalchemy import create_engine
from google.cloud import bigquery
import logging
import pandas_gbq
#from timeout_decorator import timeout
from time import sleep
import time

try:
    from modules.db_connections import initialize_db_connection
except ImportError:
    from db_connections import initialize_db_connection

class export_menuitems_data:
    def __init__(self, menu_total_buckets, menu_ntile_bucket):
        self.menu_total_buckets = menu_total_buckets
        self.menu_ntile_bucket = menu_ntile_bucket
        self.conn = initialize_db_connection()
        self.table_id = "dwh_de_test.tb_menuitems_final" 
        self.project_id = 'source_project'   
        self.client = bigquery.Client() 
        self.menuitems_df = None
        self.src_total_cnt = None
        self.tgt_total_cnt = None
        self.total_records_in_bucket = None
        self.uploaded_count = None
        self.other_uploaded_count = None
        self.delete_partial_data_ret_val = None
        self.other_ret_val = None
        self.ret_val = None
        self.chunk_size=50000
        self.total_rows_processed = 0
        self.chunk_count = 0

    def get_total_source_records(self):
        src_total_cnt_query = f"""
        SELECT COUNT(*) as total_count FROM smartdatastagdb.v_menuitems_final 
        """
        self.src_total_cnt = pd.read_sql(src_total_cnt_query,self.conn) 
        self.src_total_cnt = self.src_total_cnt['total_count'].iloc[0]

    def get_total_target_records(self):
        self.tgt_total_cnt = self.client.get_table(self.table_id).num_rows

    def final_src_tgt_verification(self):
        self.get_total_source_records()
        self.get_total_target_records()

        if int(self.src_total_cnt) == int(self.tgt_total_cnt):
            logging.info(f"Total number of records in the source table are {self.src_total_cnt} and target table {self.table_id} are {self.tgt_total_cnt} and both are matching. ")
            logging.info(f"Data export is completed successfully.")

        else:
            logging.error(f"Total number of records in the table {self.table_id} are {self.src_total_cnt} and target table {self.table_id} are {self.tgt_total_cnt} and both are not matching. Need to retrigger entire pipeline to complete the data transfer")
            raise Exception(f"Total number of records in the table {self.table_id} are {self.src_total_cnt} and target table {self.table_id} are {self.tgt_total_cnt} and both are not matching. Need to retrigger entire pipeline to complete the data transfer")          
        
    def get_curr_bucket_total_count(self):
        total_bucket_count_query = f""" 
            SELECT COUNT(*) as total_count FROM (
                SELECT *, NTILE({self.menu_total_buckets}) OVER (ORDER BY menu_item_id) AS bucket_num 
                FROM smartdatastagdb.v_menuitems_final
            ) z 
            WHERE z.bucket_num = {self.menu_ntile_bucket }  
        """
        self.total_records_in_bucket = pd.read_sql(total_bucket_count_query,self.conn)
        self.total_records_in_bucket = self.total_records_in_bucket['total_count'].iloc[0]

    def drop_bucket_num_column(self):
        try:
            drop_query = f""" ALTER TABLE `{self.table_id}` DROP COLUMN bucket_num """
            self.client.query(drop_query).result()
        except Exception as e:
            logging.error(f"Error while dropping bucket_num column from BigQuery table {self.table_id}: {str(e)}")
            raise Exception(f"Error while dropping bucket_num column from BigQuery table {self.table_id}: {str(e)}")
        
    def drop_bq_table_on_failure(self):
        try:
            drop_query = f""" DROP TABLE `{self.table_id}` """
            self.client.query(drop_query).result()
        except Exception as e:
            logging.error(f"Error while dropping BigQuery table {self.table_id}: {str(e)}")
            raise Exception(f"Error while dropping BigQuery table {self.table_id}: {str(e)}")

    def get_curr_bucket_data_from_src(self,chunk_count,chunk_size):
        try:
            bucket_data_query = f"""
                SELECT * FROM (
                    SELECT *, NTILE({self.menu_total_buckets}) OVER (ORDER BY menu_item_id) AS bucket_num 
                    FROM smartdatastagdb.v_menuitems_final
                ) z 
                WHERE z.bucket_num = {self.menu_ntile_bucket }
                LIMIT {chunk_size} OFFSET {chunk_count * chunk_size}
            """
            self.menuitems_df = pd.read_sql(bucket_data_query,self.conn)

        except Exception as e:
            logging.error(f"Error while fetching data from AlloyDB for the bucket {self.menu_ntile_bucket}: {str(e)}")
            raise Exception(f"Error while fetching data from AlloyDB for the bucket {self.menu_ntile_bucket}: {str(e)}")

    def upload_bucket_data_to_bigquery(self):
        try:
            if int(self.menu_ntile_bucket) == 1 and self.chunk_count == 0:
                logging.info(f"For the bucket {self.menu_ntile_bucket} uploading data to BigQuery with (replace mode)...")
                ret_val=pandas_gbq.to_gbq(self.menuitems_df, self.table_id, self.project_id, if_exists='replace')

                #verification pandas_gbq.to_gbq will return None in case of success else will raise an exception
                if ret_val is None: 
                    #check if the data is uploaded completely
                    uploaded_count = self.client.get_table(self.table_id).num_rows
                    # print(f"uploaded_count: {uploaded_count}")
                    # print(f"len(menuitems_df): {len(self.menuitems_df)}")
                                                    
                    if uploaded_count != len(self.menuitems_df):                             
                        logging.error(f"failed to upload the data chunk {self.chunk_count} for the bucket {self.menu_ntile_bucket}")
                        raise Exception(f"failed to upload the data chunk {self.chunk_count} for the bucket {self.menu_ntile_bucket}")
            else:
                logging.info(f"For the bucket {self.menu_ntile_bucket} uploading data to BigQuery with (append mode)...")
                other_ret_val=pandas_gbq.to_gbq(self.menuitems_df, self.table_id, self.project_id, if_exists='append')

                if other_ret_val is None:
                    logging.info(f"Successfully uploaded chunk {self.chunk_count} with {len(self.menuitems_df)} rows")
             
                else:
                    logging.error(f"failed to upload the data chunk {self.chunk_count} for the bucket {self.menu_ntile_bucket}")
                    raise Exception(f"failed to upload the data chunk {self.chunk_count} for the bucket {self.menu_ntile_bucket}")

        except Exception as e:
            logging.error(f"Error while uploading data to BigQuery for the bucket {self.menu_ntile_bucket}: {str(e)}")
            raise Exception(f"Error while uploading data to BigQuery for the bucket {self.menu_ntile_bucket}: {str(e)}") 
         
def exp_menuitems_main_function(menu_total_buckets, menu_ntile_bucket):
    try:
        export_menuitems_obj = export_menuitems_data(menu_total_buckets, menu_ntile_bucket)
        logging.info("Successfully connected to AlloyDB")
        
        export_menuitems_obj.get_curr_bucket_total_count()
        logging.info(f"Bucket {export_menuitems_obj.menu_ntile_bucket} contains {export_menuitems_obj.total_records_in_bucket} records")
        
        if export_menuitems_obj.total_records_in_bucket == 0:
            logging.warning(f"No records found in bucket {export_menuitems_obj.menu_ntile_bucket}")
            raise Exception(f"No data found in bucket {export_menuitems_obj.menu_ntile_bucket}")
        
        logging.info(f"Processing bucket {export_menuitems_obj.menu_ntile_bucket} with {export_menuitems_obj.total_records_in_bucket} records")

        logging.info(f"Fetching all data from AlloyDB for the bucket {export_menuitems_obj.menu_ntile_bucket} ...")
        
        expected_chunks = (export_menuitems_obj.total_records_in_bucket // export_menuitems_obj.chunk_size) + 1 #as taking floor division
        #max_chunks = min(expected_chunks, 1000) 
        print(f"expected_chunks: {expected_chunks}")
        #print(f"max_chunks: {max_chunks}")
        

        while export_menuitems_obj.chunk_count < expected_chunks: 
            print(f"current chunk count: {export_menuitems_obj.chunk_count}")
            print(f"total_rows_processed: {export_menuitems_obj.total_rows_processed}")

            if int(export_menuitems_obj.total_rows_processed) < int(export_menuitems_obj.total_records_in_bucket):
                export_menuitems_obj.get_curr_bucket_data_from_src(export_menuitems_obj.chunk_count,export_menuitems_obj.chunk_size)
            
                if export_menuitems_obj.menuitems_df.empty:
                    logging.error(f"No data found in bucket {export_menuitems_obj.menu_ntile_bucket} for chunk {export_menuitems_obj.chunk_count}")
                    raise Exception(f"No data found in bucket {export_menuitems_obj.menu_ntile_bucket} for chunk {export_menuitems_obj.chunk_count}")
                    
                else:
                    logging.info(f"Retrieved {len(export_menuitems_obj.menuitems_df)} rows from AlloyDB for the bucket {export_menuitems_obj.menu_ntile_bucket} chunk {export_menuitems_obj.chunk_count}")
                     
                    time.sleep(120) 
                    # Upload chunk data to BigQuerys
                    export_menuitems_obj.upload_bucket_data_to_bigquery()


                    # Update counters BEFORE checking break condition
                    export_menuitems_obj.total_rows_processed += len(export_menuitems_obj.menuitems_df)
                    export_menuitems_obj.chunk_count += 1

                    logging.info(f"Processed chunk "+str(int(export_menuitems_obj.chunk_count))+". Total rows processed: "+str(export_menuitems_obj.total_rows_processed))

            if int(export_menuitems_obj.total_rows_processed) == int(export_menuitems_obj.total_records_in_bucket):
                logging.info(f"All records have been processed successfully for the bucket {export_menuitems_obj.menu_ntile_bucket}. Total rows processed: {export_menuitems_obj.total_rows_processed}")
                break

            continue
               
        if int(export_menuitems_obj.menu_ntile_bucket) == export_menuitems_obj.menu_total_buckets:
            export_menuitems_obj.final_src_tgt_verification()
            #drop bucket_num column from bq table
            export_menuitems_obj.drop_bucket_num_column()
            exit() #successful exit

    except Exception as e:
        logging.error(f"Error during export process of bucket {export_menuitems_obj.menu_ntile_bucket} : {str(e)}")
        logging.error(f"Dropping BigQuery table {export_menuitems_obj.table_id} on failure")
        export_menuitems_obj.drop_bq_table_on_failure()
        raise Exception(f"Error during export process of bucket {export_menuitems_obj.menu_ntile_bucket} : {str(e)} retrigger entire pipeline to complete the data transfer")
   
    finally:
        # Always close the connection, even if an error occurred
        if export_menuitems_obj.conn is not None:
            try:
                export_menuitems_obj.conn.close()
                logging.info("Closed AlloyDB connection")
            except Exception as e:
                logging.warning(f"Error closing AlloyDB connection: {str(e)}") 

# def main_function(menu_total_buckets, menu_ntile_bucket):
#     try:
#         export_menuitems_obj = export_menuitems_data(menu_total_buckets, menu_ntile_bucket)
#         logging.info("Successfully connected to AlloyDB")
#         export_menuitems_obj.get_curr_bucket_total_count()
#         logging.info(f"Bucket {export_menuitems_obj.menu_ntile_bucket} contains {export_menuitems_obj.total_records_in_bucket} records")
        
#         if export_menuitems_obj.total_records_in_bucket == 0:
#             logging.warning(f"No records found in bucket {export_menuitems_obj.menu_ntile_bucket}")
#             raise Exception(f"No data found in bucket {export_menuitems_obj.menu_ntile_bucket}")
        
#         logging.info(f"Processing bucket {export_menuitems_obj.menu_ntile_bucket} with {export_menuitems_obj.total_records_in_bucket} records")

#         logging.info(f"Fetching all data from AlloyDB for the bucket {export_menuitems_obj.menu_ntile_bucket} ...")
        
#         exp_menuitems_function(export_menuitems_obj)
        
#     except Exception as timeout_error:
#         time.sleep(180)
#         export_menuitems_obj = export_menuitems_data(menu_total_buckets, menu_ntile_bucket)
#         logging.info("Successfully connected to AlloyDB")
#         export_menuitems_obj.get_curr_bucket_total_count()
#         logging.info(f"Bucket {export_menuitems_obj.menu_ntile_bucket} contains {export_menuitems_obj.total_records_in_bucket} records")
#         if export_menuitems_obj.total_records_in_bucket == 0:
#             logging.warning(f"No records found in bucket {export_menuitems_obj.menu_ntile_bucket}")
#             raise Exception(f"No data found in bucket {export_menuitems_obj.menu_ntile_bucket}")
    
#         logging.info(f"Processing bucket {export_menuitems_obj.menu_ntile_bucket} with {export_menuitems_obj.total_records_in_bucket} records")

#         logging.info(f"Fetching all data from AlloyDB for the bucket {export_menuitems_obj.menu_ntile_bucket} ...")
        
#         exp_menuitems_function(export_menuitems_obj)
        
#         logging.error(f"Error during export process of bucket {menu_ntile_bucket} : {str(e)}")
#         raise Exception(f"Error during export process of bucket {menu_ntile_bucket} : {str(e)} retrigger entire pipeline to complete the data transfer")
