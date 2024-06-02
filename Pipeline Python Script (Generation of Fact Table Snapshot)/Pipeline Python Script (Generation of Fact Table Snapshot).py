import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

# Replace these variables with your BigQuery credentials and project details
key_path = "credentials/service_account.json"
bigquery_project_id = 'my-learning-gcp-123'

# Create a BigQuery client using the service account key
credentials = service_account.Credentials.from_service_account_file(key_path)
client = bigquery.Client(credentials=credentials, project=bigquery_project_id)

# Define your SQL query
query = """
CREATE VIEW `my-learning-gcp-123.olapdb.Fact_Table_Snopshot` AS   
 SELECT  
  fb.booking_id,
  fb.from_airport_id,
  dap_from.airport_iata AS from_airport_iata, 
  dap_from.airport_icao AS from_airport_icao, 
  dap_from.airport_name AS from_airport_name, 
  dap_from.airport_city AS from_airport_city, 
  dap_from.airport_country AS from_airport_country, 
  dap_from.latitude AS from_airport_latitude, 
  dap_from.longitude AS from_airport_longitude,
  fb.to_airport_id,
  dap_to.airport_iata AS to_airport_iata, 
  dap_to.airport_icao AS to_airport_icao, 
  dap_to.airport_name AS to_airport_name, 
  dap_to.airport_city AS to_airport_city, 
  dap_to.airport_country AS to_airport_country, 
  dap_to.latitude AS to_airport_latitude, 
  dap_to.longitude AS to_airport_longitude,
  fb.airline_id,
  al.airline_iata, 
  al.airline_name, 
  al.base_airport_iata, 
  al.base_airport_name,
  fb.airplane_id,
  ap.capacity, 
  ap.type_identifier,
  fb.flight_id, 
  fl.flightno, 
  fl.departure, 
  fl.arrival,
  dd.departure_date, 
  dd.departure_year, 
  dd.departure_quarter, 
  dd.departure_month, 
  dd.departure_week,
  ad.arrival_date, 
  ad.arrival_year, 
  ad.arrival_quarter, 
  ad.arrival_month, 
  ad.arrival_week,
  dp.birthdate, 
  dp.sex, 
  dp.passenger_city, 
  dp.passenger_country,
  db.seat
FROM my-learning-gcp-123.olapdb.FACT_Booking fb
INNER JOIN my-learning-gcp-123.olapdb.DIM_Airport dap_from ON fb.from_airport_id = dap_from.airport_id
INNER JOIN my-learning-gcp-123.olapdb.DIM_Airport dap_to ON fb.to_airport_id = dap_to.airport_id
INNER JOIN my-learning-gcp-123.olapdb.DIM_Airline al ON fb.airline_id = al.airline_id
INNER JOIN my-learning-gcp-123.olapdb.DIM_Airplane ap ON fb.airplane_id = ap.airplane_id
INNER JOIN my-learning-gcp-123.olapdb.DIM_Flight fl ON fb.flight_id = fl.flight_id
INNER JOIN my-learning-gcp-123.olapdb.DIM_PassengerDetails dp ON fb.passenger_id = dp.passenger_id
INNER JOIN my-learning-gcp-123.olapdb.DIM_Booking db ON fb.booking_id = db.booking_id
INNER JOIN (SELECT date_id, date AS departure_date, year AS departure_year, quarter AS departure_quarter, month AS departure_month, week AS departure_week FROM my-learning-gcp-123.olapdb.Dim_Date) dd 
ON fb.departure_date_id = dd.date_id
INNER JOIN (SELECT date_id, date AS arrival_date, year AS arrival_year, quarter AS arrival_quarter, month AS arrival_month, week AS arrival_week FROM my-learning-gcp-123.olapdb.Dim_Date) ad 
ON fb.arrival_date_id = ad.date_id;
"""
query1= "SELECT * FROM `my-learning-gcp-123.olapdb.Fact_Table_Snopshot` limit 1000;"

# Run the query and save the result to a DataFrame
df = client.query(query)

df = client.query(query1).to_dataframe()

# Save the DataFrame to a CSV file
output_csv_path = "fact_dimension_snapshot.csv"
df.to_csv(output_csv_path, index=False)

print(f"Data saved to {output_csv_path}")
