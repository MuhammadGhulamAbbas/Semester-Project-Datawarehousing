from google.cloud import bigquery
from google.oauth2 import service_account
from ddl_sql2 import main_ddl
import logging
import os

key_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
# Create a Credentials object from the service account key file
credentials = service_account.Credentials.from_service_account_file(key_path)
# Create a BigQuery client using the credentials
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")

client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
# key_path = "credentials\service_account.json"
 

# Create a BigQuery client using the service account key
# credentials = service_account.Credentials.from_service_account_file(key_path)
 


def create_table_in_bigquery():
    table_schemas,alter_statements = main_ddl()
    
    for ddl in table_schemas:
        # print(ddl)
        logging.info(ddl)
        dataset_name=f'{ddl}'
        try:
                # Execute the insert query
                client.query(dataset_name).result()
                logging.info("Successfully  table created ")
                # print(f"Successfully  table created ")
        except Exception as e:
            logging.error(f"Failed to insert data for dataset: {dataset_name}. Error: {str(e)}")
            # print(f"Failed to insert data for dataset: {dataset_name}. Error: {str(e)}")
            
    for ddl in alter_statements:
        # print(ddl)
        logging.info(ddl)
        
        dataset_name=f'{ddl}'
        try:
                # Execute the insert query
                client.query(dataset_name).result()
                logging.info("Successfully   primay key created ")
                # print(f"Successfully   primay key created ")
        except Exception as e:
            logging.error(f"Failed to  Add Primary Key: {dataset_name}. Error: {str(e)}")
            # print(f"Failed to  Add Primary Key: {dataset_name}. Error: {str(e)}")        
        
        
# create_table_in_bigquery()        
