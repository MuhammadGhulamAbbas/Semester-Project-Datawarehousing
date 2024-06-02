import mysql.connector
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import os
import logging
# Replace these variables with your MySQL and BigQuery credentials
mysql_host = 'localhost'
port = 3307
mysql_database = 'airportdb'
mysql_user = 'root'
mysql_password = 'root'


# key_path = "credentials\service_account.json"

# Create a BigQuery client using the service account key
# credentials = service_account.Credentials.from_service_account_file(key_path)
credentials=os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
bigquery_project_id=os.environ.get('GCP_PROJECT_ID')
# bigquery_project_id = 'my-learning-gcp-123'
# bigquery_dataset_id = 'staging_airportdb' 
bigquery_dataset_id=os.environ.get('BIGQUERY_DATASET1')

def truncate_table(table_name):
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)
    table_id = f"{bigquery_project_id}.{bigquery_dataset_id}.{table_name}"
    
    query = f"TRUNCATE TABLE `{table_id}`"
    client.query(query).result()
    logging.info(f"Table {table_id} truncated successfully.")
    # print(f"Table {table_id} truncated successfully.")

def upload_to_bigquery(table_name, df):
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)
    table_id = f"{bigquery_project_id}.{bigquery_dataset_id}.{table_name}"
    
    table = client.get_table(table_id)
    schema = table.schema
    schema_field_names = [field.name for field in schema]

    # Ensure the DataFrame columns match the schema
    for field in schema_field_names:
        if field not in df.columns:
            df[field] = None  # Add missing columns with None values

    df = df[schema_field_names]  # Reorder and remove extra columns
    
         

    # Load data to BigQuery
    job = client.load_table_from_dataframe(df, table_id, job_config=bigquery.LoadJobConfig(schema=schema))
    job.result()
    logging.info(f"Data uploaded successfully to BigQuery table {table_id}")
    # print(f"Data uploaded successfully to BigQuery table {table_id}")
    
    
    
    
    # table = client.get_table(table_id)
    # schema = table.schema
    # schema_field_names = [field.name for field in schema]

    # # Ensure the DataFrame columns match the schema
    # for field in schema_field_names:
    #     if field not in df.columns:
    #         df[field] = None  # Add missing columns with None values

    # df = df[schema_field_names]  # Reorder and remove extra columns

    # # Load data to BigQuery
    # job = client.load_table_from_dataframe(df, table_id, job_config=bigquery.LoadJobConfig(schema=schema))
    # job.result()

    # print(f"Data uploaded successfully to BigQuery table {table_id}")











def main_ddl():
    connection = None
    cursor = None

    try:
        # Connect to MySQL database
        connection = mysql.connector.connect(
            host=mysql_host,
            port=port,
            database=mysql_database,
            user=mysql_user,
            password=mysql_password
        )

        if connection.is_connected():
            logging.info("Connected to MySQL database")
            # print("Connected to MySQL database")

            cursor = connection.cursor(dictionary=True)

            # Get list of tables
            original_query = """
            SELECT TABLE_SCHEMA, TABLE_NAME
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA = %s
            """
            cursor.execute(original_query, (mysql_database,))
            query_result = cursor.fetchall()

            def get_table_schema_and_data(table_name):
                full_table_name = f"{table_name}"
                logging.info(f"Processing table: {full_table_name}")
                # print(f"Processing table: {full_table_name}")

                cursor.execute(f"SELECT * FROM {table_name}")
                data = cursor.fetchall()
                return pd.DataFrame(data)

            for table in query_result:
                table_name = table['TABLE_NAME']
                df = get_table_schema_and_data(table_name)
                truncate_table(table_name)  # Truncate the table before uploading new data
                upload_to_bigquery(table_name, df)

    except mysql.connector.Error as e:
        logging.error(f"Error: {e}")
        # print(f"Error: {e}")

    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()
            logging.info("MySQL connection closed")
            # print("MySQL connection closed")

# main_ddl()






 
    # # Get the existing schema from BigQuery
    # table = client.get_table(table_id)
    # schema = table.schema
    # schema_field_names = [field.name for field in schema]

    # # Ensure the DataFrame columns match the schema and convert datatypes
    # for field in schema_field_names:
    #     if field not in df.columns:
    #         df[field] = None  # Add missing columns with None values

    # df = df[schema_field_names]  # Reorder and remove extra columns

    # # Convert DataFrame columns to appropriate types
    # for field in schema:
    #     if field.field_type == 'STRING':
    #         df[field.name] = df[field.name].astype(str)
    #     elif field.field_type == 'INTEGER':
    #         df[field.name] = pd.to_numeric(df[field.name], errors='coerce').astype('Int64')
    #     elif field.field_type == 'NUMERIC':
    #         df[field.name] = pd.to_numeric(df[field.name], errors='coerce')
    #     elif field.field_type == 'BOOLEAN':
    #         df[field.name] = df[field.name].astype('bool')
    #     elif field.field_type == 'DATE':
    #         df[field.name] = pd.to_datetime(df[field.name], errors='coerce').dt.date
    #     elif field.field_type == 'TIMESTAMP':
    #         df[field.name] = pd.to_datetime(df[field.name], errors='coerce')
    #     elif field.field_type == 'TIME':
    #         df[field.name] = pd.to_datetime(df[field.name], errors='coerce').dt.time

    # # Load data to BigQuery
    # job = client.load_table_from_dataframe(df, table_id, job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE"))
    # job.result()

    # print(f"Data uploaded successfully to BigQuery table {table_id}")

    # Get the existing schema from BigQuery








# def upload_to_bigquery(table_name,df):
     
#     client = bigquery.Client(credentials=credentials, project=credentials.project_id)
#     table_id = f"{bigquery_project_id}.{bigquery_dataset_id}.{table_name}"

#     # Get the existing schema from BigQuery
#     table = client.get_table(table_id)
#     schema = table.schema
#     schema_field_names = [field.name for field in schema]

#     # Ensure the DataFrame columns match the schema
#     for field in schema_field_names:
#         if field not in df.columns:
#             df[field] = None  # Add missing columns with None values

#     df = df[schema_field_names]  # Reorder and remove extra columns

#     # Load data to BigQuery
#     job = client.load_table_from_dataframe(df, table_id, job_config=bigquery.LoadJobConfig(schema=schema))
#     job.result()

#     print(f"Data uploaded successfully to BigQuery table {table_id}")

# def main_ddl():
#     connection = None
#     cursor = None

#     try:
#         # Connect to MySQL database
#         connection = mysql.connector.connect(
#             host=mysql_host,
#             database=mysql_database,
#             user=mysql_user,
#             password=mysql_password
#         )

#         if connection.is_connected():
#             print("Connected to MySQL database")

#             cursor = connection.cursor(dictionary=True)

#             # Get list of tables
#             original_query = """
#             SELECT TABLE_SCHEMA, TABLE_NAME
#             FROM INFORMATION_SCHEMA.TABLES
#             WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA = %s
#             """
#             cursor.execute(original_query, (mysql_database,))
#             query_result = cursor.fetchall()

#             def get_table_schema_and_data(table_name):
#                 full_table_name = f"{table_name}"
#                 print(f"Processing table: {full_table_name}")

#                 cursor.execute(f"SELECT * FROM {table_name}")
#                 data = cursor.fetchall()
#                 return pd.DataFrame(data)
            
            
#             for table in query_result:
                
#                 table_name=table['TABLE_NAME']
#                 # print(table_name)
#                 df = get_table_schema_and_data(table_name)
#                 # print(df.head())
#                 # # Example: Process and upload data for a specific table
#                 # # specific_table = 'your_specific_table_name'  # Replace with your table name
#                 # # df = get_table_schema_and_data(specific_table)
#                 upload_to_bigquery(table_name,df)

#     except mysql.connector.Error as e:
#         print(f"Error: {e}")

#     finally:
#         if cursor:
#             cursor.close()
#         if connection and connection.is_connected():
#             connection.close()
#             print("MySQL connection closed")

 

# main_ddl()





 