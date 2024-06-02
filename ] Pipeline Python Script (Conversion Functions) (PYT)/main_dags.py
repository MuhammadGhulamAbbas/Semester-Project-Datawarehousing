import os
import logging

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from create_bigquery_table import create_table_in_bigquery
from export_data_to_bigquery import main_ddl
from Facts_and_Dimensions import main
from export_data_from_OLTP_to_OLAP import main_OTP_TO_OLAP

from google.cloud import storage
 
# from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import datetime, timedelta 


default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 6, 1),
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="main_dags.py",
    # schedule_interval="@daily",
     schedule_interval="*/10 * * * *",  # Runs every 10 minutes
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['OLTP-OLAP'],
) as dag:

     

    create_table_in_bigquery = PythonOperator(
        task_id="Extracting_schemas_from_OLTP_and_create_table_in_bigquery_in_staging",
        python_callable=create_table_in_bigquery,
         
    )
    
    export_data_to_bigquery=PythonOperator(
        task_id="export_data_from_MySQL_OLTP_to_bigquery",
        python_callable=main_ddl,
         
    )

    Facts_and_Dimensions=PythonOperator(
        task_id="Create_Facts_and_Dimensions_table_in_OLAB_database",
        python_callable=main,
         
    )
    
    export_data_from_OLTP_to_OLAP=PythonOperator(
        task_id="Clean_Data_in_stagingdb_and_export_data_to_OLAP",
        python_callable=main_OTP_TO_OLAP,
         
    )
 
    



    create_table_in_bigquery >> export_data_to_bigquery  >> Facts_and_Dimensions >> export_data_from_OLTP_to_OLAP