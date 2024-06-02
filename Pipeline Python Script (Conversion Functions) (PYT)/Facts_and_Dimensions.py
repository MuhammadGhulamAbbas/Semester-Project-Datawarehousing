from google.cloud import bigquery
from google.oauth2 import service_account
import os
import logging

# BigQuery connection parameters
# key_path = "credentials/service_account.json"
# credentials = service_account.Credentials.from_service_account_file(key_path)
# credentials=os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
# client = bigquery.Client(credentials=credentials, project=credentials.project_id)

key_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
# Create a Credentials object from the service account key file
credentials = service_account.Credentials.from_service_account_file(key_path)
# Create a BigQuery client using the credentials
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")

client = bigquery.Client(credentials=credentials, project=PROJECT_ID)

bigquery_dataset_id=os.environ.get('BIGQUERY_DATASET2')
dataset_id = f"my-learning-gcp-123.{bigquery_dataset_id}"

# Define the table creation and primary key alteration statements
tables = {
    "Dim_Date": """
        CREATE TABLE IF NOT EXISTS `my-learning-gcp-123.olapdb.Dim_Date` (
            Date_ID INT64 NOT NULL,
            Date DATE NOT NULL,
            Year INT64 NOT NULL,
            Quarter INT64 NOT NULL,
            Month INT64 NOT NULL,
            Week INT64 NOT NULL
        );
        """,
        
    "Dim_Date_A": """
        CREATE TABLE IF NOT EXISTS `my-learning-gcp-123.olapdb.Dim_Date_A` (
            Date_ID INT64 NOT NULL,
            Date DATE NOT NULL,
            Year INT64 NOT NULL,
            Quarter INT64 NOT NULL,
            Month INT64 NOT NULL,
            Week INT64 NOT NULL
        );
        """,    
    "DIM_Airport": """
        CREATE TABLE IF NOT EXISTS `my-learning-gcp-123.olapdb.DIM_Airport` (
            airport_id INT64 NOT NULL,
            airport_iata STRING,
            airport_icao STRING NOT NULL,
            airport_name STRING NOT NULL,
            airport_city STRING,
            airport_country STRING,
            latitude NUMERIC NOT NULL,
            longitude NUMERIC NOT NULL
        );
        """,
    "DIM_Airport_to": """
        CREATE TABLE IF NOT EXISTS `my-learning-gcp-123.olapdb.DIM_Airport_to` (
            airport_id INT64 NOT NULL,
            airport_iata STRING,
            airport_icao STRING NOT NULL,
            airport_name STRING NOT NULL,
            airport_city STRING,
            airport_country STRING,
            latitude NUMERIC NOT NULL,
            longitude NUMERIC NOT NULL
        );
        """, 
    "DIM_Airport_from": """
        CREATE TABLE IF NOT EXISTS `my-learning-gcp-123.olapdb.DIM_Airport_from` (
            airport_id INT64 NOT NULL,
            airport_iata STRING,
            airport_icao STRING NOT NULL,
            airport_name STRING NOT NULL,
            airport_city STRING,
            airport_country STRING,
            latitude NUMERIC NOT NULL,
            longitude NUMERIC NOT NULL
        );
        """,       
    "DIM_Airline": """
        CREATE TABLE IF NOT EXISTS `my-learning-gcp-123.olapdb.DIM_Airline` (
            airline_id INT64 NOT NULL,
            Airline_iata STRING,
            airline_name STRING,
            base_airport_iata STRING,
            base_airport_name STRING NOT NULL
        );
        """,
    "DIM_Airplane": """
        CREATE TABLE IF NOT EXISTS `my-learning-gcp-123.olapdb.DIM_Airplane` (
            airplane_id INT64 NOT NULL,
            capacity INT64 NOT NULL,
            type_identifier STRING
        );
        """,
    "DIM_Flight": """
        CREATE TABLE IF NOT EXISTS `my-learning-gcp-123.olapdb.DIM_Flight` (
            flight_id INT64 NOT NULL,
            flightno STRING NOT NULL,
            departure TIMESTAMP NOT NULL,
            arrival TIMESTAMP NOT NULL
        );
        """,
    "DIM_PassengerDetails": """
        CREATE TABLE IF NOT EXISTS `my-learning-gcp-123.olapdb.DIM_PassengerDetails` (
            passenger_id INT64 NOT NULL,
            birthdate DATE NOT NULL,
            sex STRING,
            passenger_city STRING NOT NULL,
            passenger_country STRING NOT NULL
        );
        """,
        
     "DIM_Booking": """
        CREATE TABLE IF NOT EXISTS `my-learning-gcp-123.olapdb.DIM_Booking` (
            booking_id INT64 NOT NULL ,
            seat STRING
        );
        """,
         
             
    "FACT_Booking": """
        CREATE TABLE IF NOT EXISTS `my-learning-gcp-123.olapdb.FACT_Booking` (
            booking_ID INT64 NOT NULL,
            from_airport_id INT64 NOT NULL,
            to_airport_id INT64 NOT NULL,
            airline_id INT64 NOT NULL,
            airplane_id INT64 NOT NULL,
            flight_id INT64 NOT NULL,
            departure_date_id INT64 NOT NULL,
            arrival_date_id INT64 NOT NULL,
            passenger_id INT64 NOT NULL,
            Price NUMERIC NOT NULL,
            capacity INT64 NOT NULL
             
        );
        """
}

primary_keys = {
    "Dim_Date": "ALTER TABLE `my-learning-gcp-123.olapdb.Dim_Date` ADD PRIMARY KEY (Date_ID) NOT ENFORCED;",
    "Dim_Date_A": "ALTER TABLE `my-learning-gcp-123.olapdb.Dim_Date_A` ADD PRIMARY KEY (Date_ID) NOT ENFORCED;",
    "DIM_Airport": "ALTER TABLE `my-learning-gcp-123.olapdb.DIM_Airport` ADD PRIMARY KEY (airport_id) NOT ENFORCED;",
    "DIM_Airport_from": "ALTER TABLE `my-learning-gcp-123.olapdb.DIM_Airport_from` ADD PRIMARY KEY (airport_id) NOT ENFORCED;",
    "DIM_Airport_to": "ALTER TABLE `my-learning-gcp-123.olapdb.DIM_Airport_to` ADD PRIMARY KEY (airport_id) NOT ENFORCED;",
    "DIM_Airline": "ALTER TABLE `my-learning-gcp-123.olapdb.DIM_Airline` ADD PRIMARY KEY (airline_id) NOT ENFORCED;",
    "DIM_Airplane": "ALTER TABLE `my-learning-gcp-123.olapdb.DIM_Airplane` ADD PRIMARY KEY (airplane_id) NOT ENFORCED;",
    "DIM_Flight": "ALTER TABLE `my-learning-gcp-123.olapdb.DIM_Flight` ADD PRIMARY KEY (flight_id) NOT ENFORCED;",
    "DIM_PassengerDetails": "ALTER TABLE `my-learning-gcp-123.olapdb.DIM_PassengerDetails` ADD PRIMARY KEY (passenger_id) NOT ENFORCED;",
    "DIM_Booking":"ALTER TABLE `my-learning-gcp-123.olapdb.DIM_Booking` ADD PRIMARY KEY (booking_id) NOT ENFORCED;",
    "FACT_Booking": "ALTER TABLE `my-learning-gcp-123.olapdb.FACT_Booking` ADD PRIMARY KEY (booking_ID) NOT ENFORCED;"
}

foreign_keys_query = """
ALTER TABLE `my-learning-gcp-123.olapdb.FACT_Booking`
ADD CONSTRAINT fk_from_airport_id FOREIGN KEY (from_airport_id) REFERENCES `my-learning-gcp-123.olapdb.DIM_Airport`(airport_id) NOT ENFORCED,
ADD CONSTRAINT fk_to_airport_id FOREIGN KEY (to_airport_id) REFERENCES `my-learning-gcp-123.olapdb.DIM_Airport`(airport_id) NOT ENFORCED,
ADD CONSTRAINT fk_airline_id FOREIGN KEY (airline_id) REFERENCES `my-learning-gcp-123.olapdb.DIM_Airline`(airline_id) NOT ENFORCED,
ADD CONSTRAINT fk_airplane_id FOREIGN KEY (airplane_id) REFERENCES `my-learning-gcp-123.olapdb.DIM_Airplane`(airplane_id) NOT ENFORCED,
ADD CONSTRAINT fk_passenger_id FOREIGN KEY (passenger_id) REFERENCES `my-learning-gcp-123.olapdb.DIM_PassengerDetails`(passenger_id) NOT ENFORCED,
ADD CONSTRAINT fk_departure_date_id FOREIGN KEY (departure_date_id) REFERENCES `my-learning-gcp-123.olapdb.Dim_Date`(date_id) NOT ENFORCED,
ADD CONSTRAINT fk_arrival_date_id FOREIGN KEY (arrival_date_id) REFERENCES `my-learning-gcp-123.olapdb.Dim_Date`(date_id) NOT ENFORCED,
ADD CONSTRAINT fk_flight_id FOREIGN KEY (flight_id) REFERENCES `my-learning-gcp-123.olapdb.DIM_Flight`(flight_id) NOT ENFORCED;
"""



# Function to execute a BigQuery statement
def execute_query(query):
    try:
        client.query(query).result()
    except Exception as e:
        logging.error(f"Error executing query: {query}\nError: {e}")
        
        # print(f"Error executing query: {query}\nError: {e}")

def create_table(table_name, create_statement):
    logging.info(f"Creating table {table_name}...")
    # print(f"Creating table {table_name}...")
    execute_query(create_statement)
    logging.info(f"Table {table_name} created.")
    # print(f"Table {table_name} created.")

 
def add_primary_key(table_name, alter_statement):
    logging.info(f"Adding primary key to table {table_name}....")
    # print(f"Adding primary key to table {table_name}...")
    execute_query(alter_statement)
    logging.info(f"Primary key added to table {table_name}.")
    # print(f"Primary key added to table {table_name}.")


def add_foreign_keys():
    logging.info("Adding foreign keys...")
    # print("Adding foreign keys...")
    execute_query(foreign_keys_query)
    logging.info("Foreign keys added.")
    # print("Foreign keys added.")
    
# Main function to create tables and add primary keys
def main():
    for table_name, create_statement in tables.items():
        create_table(table_name, create_statement)
        
        if table_name in primary_keys:
            add_primary_key(table_name, primary_keys[table_name])
    logging.info("All tables created and primary keys added.")
    # print("All tables created and primary keys added.")
    add_foreign_keys()

main()