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

class export_establishment_data:
    def __init__(self, est_total_buckets, est_ntile_bucket):
        self.est_total_buckets = est_total_buckets
        self.est_ntile_bucket = est_ntile_bucket
        self.conn = initialize_db_connection()
        #self.table_id = "dwh_de_test.tb_fb_delelivery_data_establishment_new" 
        self.table_id = "dwh_de_test.stamm_e_export_gbq"
        self.project_id = 'source_project'   
        self.client = bigquery.Client() 
        self.est_df = None
        self.src_total_cnt = None
        self.tgt_total_cnt = None
        self.total_records_in_bucket = None
        self.uploaded_count = None
        self.other_uploaded_count = None
        self.delete_partial_data_ret_val = None
        self.other_ret_val = None
        self.ret_val = None
        
    #updated source view smartdatastagdb.v_fb_delivery_data_establishment_new to smartdata_analyticdb.stamm_e_export
    def get_total_source_records(self):
        src_total_cnt_query = f"""
        SELECT COUNT(*) as total_count FROM smartdata_analyticdb.stamm_e_export
        """
        self.src_total_cnt = pd.read_sql(src_total_cnt_query,self.conn)
        self.src_total_cnt = self.src_total_cnt['total_count'].iloc[0]

    def get_total_target_records(self):
        self.tgt_total_cnt = self.client.get_table(self.table_id).num_rows
    
    def get_curr_bucket_total_count(self):
        total_bucket_count_query = f""" 
            SELECT COUNT(*) as total_count FROM (
                SELECT *, NTILE({self.est_total_buckets}) OVER (ORDER BY md_establishment_id) AS bucket_num 
                FROM smartdata_analyticdb.stamm_e_export
            ) z 
            WHERE z.bucket_num = {self.est_ntile_bucket }  
        """
        self.total_records_in_bucket = pd.read_sql(total_bucket_count_query,self.conn)
        self.total_records_in_bucket = self.total_records_in_bucket['total_count'].iloc[0]
    
    def get_curr_bucket_data_from_src(self):
        bucket_data_query = f"""
            SELECT * FROM (
                SELECT *, NTILE({self.est_total_buckets}) OVER (ORDER BY md_establishment_id) AS bucket_num 
                FROM smartdata_analyticdb.stamm_e_export
            ) z 
            WHERE z.bucket_num = {self.est_ntile_bucket }
        """
        self.est_df = pd.read_sql(bucket_data_query,self.conn)

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

    def upload_bucket1_data_to_bigquery(self):
        try:
            logging.info(f"For the bucket {self.est_ntile_bucket} uploading data to BigQuery with (replace mode)...")
            ret_val=pandas_gbq.to_gbq(self.est_df, self.table_id, self.project_id, if_exists='replace')
            
            #verification pandas_gbq.to_gbq will return None in case of success else will raise an exception
            if ret_val is None: 
                #check if the data is uploaded completely
                uploaded_count = self.client.get_table(self.table_id).num_rows
                # print(f"uploaded_count: {uploaded_count}")
                # print(f"len(est_df): {len(self.est_df)}")
                                                
                if uploaded_count != len(self.est_df):                             
                    logging.error(f"For the bucket {self.est_ntile_bucket} failed to upload {len(self.est_df)} rows to BigQuery table {self.table_id} or has partial data")
                    raise Exception(f"For the bucket {self.est_ntile_bucket} failed to upload {len(self.est_df)} rows to BigQuery table {self.table_id} or has partial data")
                
                logging.info(f"For the bucket {self.est_ntile_bucket} uploaded rows to BigQuery table are {uploaded_count} and expected rows are {len(self.est_df)} and both are matching. ")
                logging.info(f"For the bucket {self.est_ntile_bucket} successfully uploaded {len(self.est_df)} rows to BigQuery table {self.table_id} and verified the count.")
            
        except Exception as e:
            logging.error(f"Error while uploading data to BigQuery for the bucket {self.est_ntile_bucket}: {str(e)}")
            raise Exception(f"Error while uploading data to BigQuery for the bucket {self.est_ntile_bucket}: {str(e)}") 
       
    def upload_other_bucket_data_to_bigquery(self):
        try:       
            logging.info(f"For the bucket {self.est_ntile_bucket} uploading data to BigQuery with (append mode)...")
            other_ret_val=pandas_gbq.to_gbq(self.est_df, self.table_id, self.project_id, if_exists='append')

            if other_ret_val is None:
                #check if the data is uploaded completely
                # lst_ids=[]
                # lst_ids = self.est_df['md_establishment_id'].tolist()
                verify_query = f"""
                SELECT COUNT(*) as count 
                FROM `{self.table_id}` 
                WHERE bucket_num = {self.est_ntile_bucket}
                """
                
                verify_result = self.client.query(verify_query).to_dataframe()
                other_uploaded_count = verify_result['count'].iloc[0]
                #print(f"other_uploaded_count: {other_uploaded_count}")
                logging.info(f"other_uploaded_count: {other_uploaded_count}")
                                                                                            
                if int(other_uploaded_count) == len(self.est_df):
                    logging.info(f"For the bucket {self.est_ntile_bucket} uploaded rows to BigQuery table are {other_uploaded_count} and expected rows are {len(self.est_df)} and both are matching. ")
                    logging.info(f"For the bucket {self.est_ntile_bucket} successfully uploaded {len(self.est_df)} rows to BigQuery table {self.table_id} and verified the count.")

                    #after last bucket load verifying the total number of records in the table
                    if int(self.est_ntile_bucket) == self.est_total_buckets:
                        self.get_total_source_records()
                        self.get_total_target_records()

                        if int(self.src_total_cnt) == int(self.tgt_total_cnt):
                            logging.info(f"Total number of records in the source table are {self.src_total_cnt} and target table {self.table_id} are {self.tgt_total_cnt} and both are matching. ")
                            logging.info(f"Data export is completed successfully.")

                            #drop bucket_num column from bq table
                            self.drop_bucket_num_column()
                            exit() #successful exit

                        else:
                            logging.error(f"Total number of records in the table {self.table_id} are {self.src_total_cnt} and target table {self.table_id} are {self.tgt_total_cnt} and both are not matching. Need to retrigger entire pipeline to complete the data transfer")
                            raise Exception(f"Total number of records in the table {self.table_id} are {self.src_total_cnt} and target table {self.table_id} are {self.tgt_total_cnt} and both are not matching. Need to retrigger entire pipeline to complete the data transfer")
                    else:
                        exit() #successful exit
                                                
                else:
                    #delete partial data from the table if not uploaded completely
                    # del_lst_ids=[]
                    # del_lst_ids = self.est_df['md_establishment_id'].tolist()
                    delete_partial_data_query = f"""
                                    DELETE FROM `{self.table_id}` 
                                    WHERE bucket_num = {self.est_ntile_bucket}
                                    """
                    delete_partial_data_ret_val=self.client.query(delete_partial_data_query) 
                    delete_partial_data_ret_val.result()
                    logging.info(f"deleted rows {delete_partial_data_ret_val.num_dml_affected_rows} from BigQuery table {self.table_id}")

                    if delete_partial_data_ret_val.num_dml_affected_rows == len(self.est_df):
                        logging.info(f"For the bucket {self.est_ntile_bucket} successfully deleted partial data from BigQuery table {self.table_id}")
                        logging.info(f"Only retrigger this bucket {self.est_ntile_bucket} to complete the data transfer")
                        exit() #successful exit
                    else:
                        logging.error(f"Failed to delete partially uploaded data from BigQuery table {self.table_id}. Need to retrigger entire pipeline to complete the data transfer")
                        raise Exception(f"For the bucket {self.est_ntile_bucket} failed to upload {len(self.est_df)} rows to BigQuery table {self.table_id} or has partial data")
                
        except Exception as e:
            logging.error(f"Error while uploading data to BigQuery for the bucket {self.est_ntile_bucket}: {str(e)}")
            raise Exception(f"Error while uploading data to BigQuery for the bucket {self.est_ntile_bucket}: {str(e)}")   
         
def exp_establishment_main_function(est_total_buckets, est_ntile_bucket):
    try:
        export_est_obj = export_establishment_data(est_total_buckets, est_ntile_bucket)
        logging.info("Successfully connected to AlloyDB")
        
        export_est_obj.get_curr_bucket_total_count()
        logging.info(f"Bucket {export_est_obj.est_ntile_bucket} contains {export_est_obj.total_records_in_bucket} records")
        
        if export_est_obj.total_records_in_bucket == 0:
            logging.warning(f"No records found in bucket {export_est_obj.est_ntile_bucket}")
            raise Exception(f"No data found in bucket {export_est_obj.est_ntile_bucket}")
        
        logging.info(f"Processing bucket {export_est_obj.est_ntile_bucket} with {export_est_obj.total_records_in_bucket} records")

        logging.info(f"Fetching all data from AlloyDB for the bucket {export_est_obj.est_ntile_bucket} ...")
        try:
            export_est_obj.get_curr_bucket_data_from_src()
        except Exception as e:
            logging.error(f"Error while fetching data from AlloyDB for the bucket {export_est_obj.est_ntile_bucket}: {str(e)}")
            raise Exception(f"Error while fetching data from AlloyDB for the bucket {export_est_obj.est_ntile_bucket}: {str(e)}")
        
        if export_est_obj.est_df.empty:
            logging.warning(f"No data found in bucket {export_est_obj.est_ntile_bucket}")
            raise Exception(f"No data found in bucket {export_est_obj.est_ntile_bucket}")
            
        else:
            if export_est_obj.total_records_in_bucket != len(export_est_obj.est_df):
                logging.warning(f"AlloyDB Data fetch for the bucket {export_est_obj.est_ntile_bucket} failed as Total number of records in bucket does not match the total number of records in the df")
                raise Exception(f"AlloyDB Data fetch for the bucket {export_est_obj.est_ntile_bucket} failed as Total number of records in bucket does not match the total number of records in the df")
            
            else:
                logging.info(f"Data fetch for the bucket {export_est_obj.est_ntile_bucket} is successful and matches the total number of records in the df")
                
                logging.info(f"Retrieved {len(export_est_obj.est_df)} rows from AlloyDB for the bucket {export_est_obj.est_ntile_bucket}")

                # Upload all data to BigQuery
                # for first bucket we are using replace mode
                if int(export_est_obj.est_ntile_bucket) == 1:
                    export_est_obj.upload_bucket1_data_to_bigquery()
                elif int(export_est_obj.est_ntile_bucket) > 1 and int(export_est_obj.est_ntile_bucket) <= export_est_obj.est_total_buckets:   
                    export_est_obj.upload_other_bucket_data_to_bigquery()
    
    except Exception as e:
        logging.error(f"Error during export process of bucket {export_est_obj.est_ntile_bucket} : {str(e)}")
        #logging.error(f"Dropping BigQuery table {export_est_obj.table_id} on failure")
        #export_est_obj.drop_bq_table_on_failure()
        raise Exception(f"Error during export process of bucket {export_est_obj.est_ntile_bucket} : {str(e)} retrigger entire pipeline to complete the data transfer")
   
    finally:
        # Always close the connection, even if an error occurred
        if export_est_obj.conn is not None:
            try:
                export_est_obj.conn.close()
                logging.info("Closed AlloyDB connection")
            except Exception as e:
                logging.warning(f"Error closing AlloyDB connection: {str(e)}") 



    