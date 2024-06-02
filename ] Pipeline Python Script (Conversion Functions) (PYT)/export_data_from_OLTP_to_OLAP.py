
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import os
 
import logging

# Define your OLTP and OLAP dataset names
oltp_dataset = 'my-learning-gcp-123.staging_airportdb'
olap_dataset = 'my-learning-gcp-123.olapdb'

 
key_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
# Create a Credentials object from the service account key file
credentials = service_account.Credentials.from_service_account_file(key_path)
# Create a BigQuery client using the credentials
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")

client = bigquery.Client(credentials=credentials, project=PROJECT_ID)

def clean_table(df, drop_columns=None, numerical_impute_strategy='mean', categorical_impute_strategy='mode'):
    # Return the empty DataFrame as is
    if df.empty:
        return df
    # Drop specified columns if provided
    if drop_columns:
        df.drop(drop_columns, axis=1, inplace=True)
    
    # Separate numerical and categorical columns
    numerical_cols = df.select_dtypes(include=['number']).columns
    categorical_cols = df.select_dtypes(include=['object']).columns

    # Impute missing values for numerical columns
    if numerical_impute_strategy == 'mean':
        df[numerical_cols] = df[numerical_cols].fillna(df[numerical_cols].mean())
    elif numerical_impute_strategy == 'median':
        df[numerical_cols] = df[numerical_cols].fillna(df[numerical_cols].median())

    # Impute missing values for categorical columns
    if categorical_impute_strategy == 'mode':
        df[categorical_cols] = df[categorical_cols].apply(lambda x: x.fillna(x.mode()[0]))

    return df


# Clean and upload each table
def clean_data_upload_to_staging():
    # Retrieve the list of tables in the dataset
    tables = client.list_tables(oltp_dataset)
    
    for table in tables:
        table_name = table.table_id
        # Load the data from BigQuery
        query = f"SELECT * FROM `{oltp_dataset}.{table_name}`"
        df = client.query(query).to_dataframe()

        # Clean the data
        df_cleaned = clean_table(df)

        # Upload the cleaned data back to BigQuery
        table_id = f"{oltp_dataset}.{table_name}"
        try:
            job = client.load_table_from_dataframe(df_cleaned, table_id, job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE"))
            job.result()
            logging.info(f"Table {table_name} cleaned and uploaded successfully.")
            # print(f"Table {table_name} cleaned and uploaded successfully.")
        except Exception as e:
            logging.error(f"Failed to upload table {table_name} due to {e}")
            # print(f"Failed to upload table {table_name} due to {e}")
    
    logging.info("Data cleaning and upload process completed successfully.")
    # print("Data cleaning and upload process completed successfully.")

# After cleaning and uploading the data, execute the SQL script to transfer data to OLAP tables
def insert_data_to_olap():
    sql_script = """
    -- Copy data to DIM_Date
    INSERT INTO `my-learning-gcp-123.olapdb.Dim_Date` (Date_ID, Date, Year, Quarter, Month, Week)
    SELECT
    DATE_DIFF(CAST(s.departure AS DATE), DATE '1970-01-01', DAY) AS Date_ID,
    CAST(s.departure AS DATE) AS Date,
    EXTRACT(YEAR FROM s.departure) AS Year,
    EXTRACT(QUARTER FROM s.departure) AS Quarter,
    EXTRACT(MONTH FROM s.departure) AS Month,
    EXTRACT(WEEK FROM s.departure) AS Week
    FROM `my-learning-gcp-123.staging_airportdb.flight` s
    WHERE NOT EXISTS (
    SELECT 1
    FROM `my-learning-gcp-123.olapdb.Dim_Date` d
    WHERE d.Date = CAST(s.departure AS DATE)
    );
    
     -- Copy data to DIM_Date_A
    INSERT INTO `my-learning-gcp-123.olapdb.Dim_Date_A` (Date_ID, Date, Year, Quarter, Month, Week)
    SELECT
    DATE_DIFF(CAST(s.departure AS DATE), DATE '1970-01-01', DAY) AS Date_ID,
    CAST(s.departure AS DATE) AS Date,
    EXTRACT(YEAR FROM s.departure) AS Year,
    EXTRACT(QUARTER FROM s.departure) AS Quarter,
    EXTRACT(MONTH FROM s.departure) AS Month,
    EXTRACT(WEEK FROM s.departure) AS Week
    FROM `my-learning-gcp-123.staging_airportdb.flight` s
    WHERE NOT EXISTS (
    SELECT 1
    FROM `my-learning-gcp-123.olapdb.Dim_Date_A` d
    WHERE d.Date = CAST(s.departure AS DATE)
    );

    -- Copy data to DIM_Airport
    INSERT INTO `my-learning-gcp-123.olapdb.DIM_Airport` (airport_id, airport_iata, airport_icao, airport_name, airport_city, airport_country, latitude, longitude)
    SELECT s.airport_id, s.iata, s.icao, s.name, g.city, g.country, g.latitude, g.longitude
    FROM `my-learning-gcp-123.staging_airportdb.airport` s
    JOIN `my-learning-gcp-123.staging_airportdb.airport_geo` g
    ON s.airport_id = g.airport_id
    WHERE NOT EXISTS (
    SELECT 1
    FROM `my-learning-gcp-123.olapdb.DIM_Airport` d
    WHERE d.airport_id = s.airport_id
    );
    
     -- Copy data to DIM_Airport_from
    INSERT INTO `my-learning-gcp-123.olapdb.DIM_Airport_from` (airport_id, airport_iata, airport_icao, airport_name, airport_city, airport_country, latitude, longitude)
    SELECT s.airport_id, s.iata, s.icao, s.name, g.city, g.country, g.latitude, g.longitude
    FROM `my-learning-gcp-123.staging_airportdb.airport` s
    JOIN `my-learning-gcp-123.staging_airportdb.airport_geo` g
    ON s.airport_id = g.airport_id
    WHERE NOT EXISTS (
    SELECT 1
    FROM `my-learning-gcp-123.olapdb.DIM_Airport_from` d
    WHERE d.airport_id = s.airport_id
    );
    
    -- Copy data to DIM_Airport_to
    INSERT INTO `my-learning-gcp-123.olapdb.DIM_Airport_to` (airport_id, airport_iata, airport_icao, airport_name, airport_city, airport_country, latitude, longitude)
    SELECT s.airport_id, s.iata, s.icao, s.name, g.city, g.country, g.latitude, g.longitude
    FROM `my-learning-gcp-123.staging_airportdb.airport` s
    JOIN `my-learning-gcp-123.staging_airportdb.airport_geo` g
    ON s.airport_id = g.airport_id
    WHERE NOT EXISTS (
    SELECT 1
    FROM `my-learning-gcp-123.olapdb.DIM_Airport_to` d
    WHERE d.airport_id = s.airport_id
    );

    -- Copy data to DIM_Airline
    INSERT INTO `my-learning-gcp-123.olapdb.DIM_Airline` (airline_id, Airline_iata, airline_name, base_airport_iata, base_airport_name)
    SELECT s.airline_id, s.iata, s.airlinename, a.iata, a.name
    FROM `my-learning-gcp-123.staging_airportdb.airline` s
    JOIN `my-learning-gcp-123.staging_airportdb.airport` a
    ON s.base_airport = a.airport_id
    WHERE NOT EXISTS (
    SELECT 1
    FROM `my-learning-gcp-123.olapdb.DIM_Airline` d
    WHERE d.airline_id = s.airline_id
    );

    -- Copy data to DIM_Airplane
    INSERT INTO `my-learning-gcp-123.olapdb.DIM_Airplane` (airplane_id, capacity, type_identifier)
    SELECT s.airplane_id, s.capacity, t.identifier
    FROM `my-learning-gcp-123.staging_airportdb.airplane` s
    JOIN `my-learning-gcp-123.staging_airportdb.airplane_type` t
    ON s.type_id = t.type_id
    WHERE NOT EXISTS (
    SELECT 1
    FROM `my-learning-gcp-123.olapdb.DIM_Airplane` d
    WHERE d.airplane_id = s.airplane_id
    );

    -- Copy data to DIM_Flight
    INSERT INTO `my-learning-gcp-123.olapdb.DIM_Flight` (flight_id, flightno, departure, arrival)
    SELECT s.flight_id, s.flightno, s.departure, s.arrival
    FROM `my-learning-gcp-123.staging_airportdb.flight` s
    WHERE NOT EXISTS (
    SELECT 1
    FROM `my-learning-gcp-123.olapdb.DIM_Flight` d
    WHERE d.flight_id = s.flight_id
    );

    -- Copy data to DIM_PassengerDetails
    INSERT INTO `my-learning-gcp-123.olapdb.DIM_PassengerDetails` (passenger_id, birthdate, sex, passenger_city, passenger_country)
    SELECT s.passenger_id, s.birthdate, s.sex, s.city, s.country
    FROM `my-learning-gcp-123.staging_airportdb.passengerdetails` s
    WHERE NOT EXISTS (
    SELECT 1
    FROM `my-learning-gcp-123.olapdb.DIM_PassengerDetails` d
    WHERE d.passenger_id = s.passenger_id
    );
    
    -- Copy data to DIM_Booking
    INSERT INTO `my-learning-gcp-123.olapdb.DIM_Booking` (booking_id,seat)
    select s.booking_id ,s.seat FROM `my-learning-gcp-123.staging_airportdb.booking` s
    WHERE NOT EXISTS (
    SELECT 1   FROM `my-learning-gcp-123.olapdb.DIM_Booking` d
    WHERE d.booking_id = s.booking_id
    );
    

    -- Copy data to FACT_Booking
    INSERT INTO `my-learning-gcp-123.olapdb.FACT_Booking` (booking_ID, from_airport_id, to_airport_id, airline_id, airplane_id, flight_id, departure_date_id, arrival_date_id, passenger_id, Price,capacity)
    SELECT s.booking_id, f.from_, f.to_, f.airline_id, f.airplane_id, s.flight_id,
    DATE_DIFF(CAST(f.departure AS DATE), DATE '1970-01-01', DAY) AS departure_date_id,
    DATE_DIFF(CAST(f.arrival AS DATE), DATE '1970-01-01', DAY) AS arrival_date_id,
    s.passenger_id, s.price, a.capacity
    FROM `my-learning-gcp-123.staging_airportdb.booking` s
    JOIN `my-learning-gcp-123.staging_airportdb.flight` f
    ON s.flight_id = f.flight_id
    JOIN  `my-learning-gcp-123.staging_airportdb.airplane` a
    ON a.airplane_id = f.airplane_id
    WHERE NOT EXISTS (
    SELECT 1
    FROM `my-learning-gcp-123.olapdb.FACT_Booking` d
    WHERE d.booking_ID = s.booking_id
    );
    """
    queries = sql_script.split(';')

    for query in queries:
        if query.strip():
            try:
                client.query(query).result()
                logging.info(f"Query executed successfully:\n{query}")
                # print(f"Query executed successfully:\n{query}")
            except Exception as e:
                logging.error(f"Failed to execute query:\n{query}\nDue to {e}")
                # print(f"Failed to execute query:\n{query}\nDue to {e}")
    
    logging.info("Data transfer from staging to OLAP tables completed successfully.")
    # print("Data transfer from staging to OLAP tables completed successfully.")

def main_OTP_TO_OLAP():
    clean_data_upload_to_staging()
    insert_data_to_olap()

 
# main_OTP_TO_OLAP()
