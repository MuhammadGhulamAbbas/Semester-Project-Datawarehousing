from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd

# Define your OLAP dataset name
olap_dataset = 'my-learning-gcp-123.olapdb'

key_path = "credentials/service_account.json"

# Create a BigQuery client using the service account key
credentials = service_account.Credentials.from_service_account_file(key_path)

# Initialize the BigQuery client
client = bigquery.Client(credentials=credentials, project=credentials.project_id)

# Define the queries
queries = {
    "total_revenue": """
        SELECT AF.airport_city, D.Year, D.Month, SUM(F.Price) AS Total_Revenue
        FROM `my-learning-gcp-123.olapdb.FACT_Booking` F 
        JOIN `my-learning-gcp-123.olapdb.DIM_Airport` AF ON F.from_airport_id = AF.airport_id
        JOIN `my-learning-gcp-123.olapdb.Dim_Date` D ON F.departure_date_id = D.Date_ID 
        WHERE AF.airport_city = 'ARVIKA'
        AND D.Year = 2015
        AND D.Month = 6
        GROUP BY AF.airport_city, D.Year, D.Month;
    """,
    "passenger_count": """
        SELECT 
        AF.airport_iata AS from_airport_iata, 
        AF.airport_name AS from_airport_name,
        ATO.airport_iata AS to_airport_iata, 
        ATO.airport_name AS to_airport_name, 
        COUNT(F.passenger_id) AS passenger_count
        FROM 
            `my-learning-gcp-123.olapdb.FACT_Booking` F 
        JOIN 
            `my-learning-gcp-123.olapdb.DIM_Airport` AF ON F.from_airport_id = AF.airport_id
        JOIN 
            `my-learning-gcp-123.olapdb.DIM_Airport` ATO ON F.to_airport_id = ATO.airport_id

        WHERE 
            AF.airport_iata = 'TAO'
            AND ATO.airport_iata = 'JSR'    

        GROUP BY
            AF.airport_iata, 
            AF.airport_name,
            ATO.airport_iata, 
            ATO.airport_name;
    """,
    "average_capacity_utilization": """
        WITH CTE_1 AS (
        SELECT AL.airline_iata, AL.airline_name, F.airplane_id, 
        COUNT(F.booking_id) AS Passenger_count, D.Year, D.Week
        FROM `my-learning-gcp-123.olapdb.FACT_Booking` F 
        JOIN `my-learning-gcp-123.olapdb.Dim_Date` D ON F.departure_date_id = D.Date_ID 
        JOIN `my-learning-gcp-123.olapdb.DIM_Airline` AL ON F.airline_id = AL.airline_id
        WHERE AL.airline_iata = 'SP'
        AND D.Year = 2015
        AND D.Week = 23
        GROUP BY D.Year, D.Week, AL.airline_iata, AL.airline_name, F.airplane_id
    ),
    CTE_2 AS (
        SELECT F.airplane_id,  
        MAX(AP.capacity) AS Airplane_Capacity, D.Year, D.Week
        FROM `my-learning-gcp-123.olapdb.FACT_Booking` F 
        JOIN `my-learning-gcp-123.olapdb.Dim_Date` D ON F.departure_date_id = D.Date_ID
        JOIN `my-learning-gcp-123.olapdb.DIM_Airline` AL ON F.airline_id = AL.airline_id 
        JOIN `my-learning-gcp-123.olapdb.DIM_Airplane` AP ON F.airplane_id = AP.airplane_id
        WHERE AL.airline_iata = 'SP'
        AND D.Year = 2015
        AND D.Week = 23
        GROUP BY D.Year, D.Week, AL.airline_iata, AL.airline_name, F.airplane_id
    )
    SELECT CTE_1.airline_iata, CTE_1.airline_name,
    AVG(CTE_1.Passenger_count / CTE_2.Airplane_Capacity) AS Avg_Capacity_Utilization
    FROM CTE_1
    JOIN CTE_2 ON CTE_1.airplane_id = CTE_2.airplane_id
    GROUP BY CTE_1.airline_iata, CTE_1.airline_name;
 
    """,
    "gender_ratio": """
        SELECT 
        SUM(CASE WHEN P.sex = 'm' THEN 1 ELSE 0 END) / COUNT(F.passenger_id) AS ratio_men, 
        SUM(CASE WHEN P.sex = 'w' THEN 1 ELSE 0 END) / COUNT(F.passenger_id) AS ratio_women
        FROM `my-learning-gcp-123.olapdb.FACT_Booking` F 
        JOIN `my-learning-gcp-123.olapdb.DIM_PassengerDetails` P ON F.passenger_id = P.passenger_id;
    """
}

# Execute the queries and print the results
for query_name, query in queries.items():
    print(f"Executing query: {query_name}")
    try:
        query_job = client.query(query)
        results = query_job.result()
        df = results.to_dataframe()
        print(f"Results for {query_name}:")
        print(df)
        print("\n")
    except Exception as e:
        print(f"Failed to execute query: {query_name}")
        print(f"Error: {e}\n")

 
print("Query execution completed.")
