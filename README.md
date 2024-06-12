# Semester-Project-Datawarehousing
Data Warehousing Project
Implement a data mart providing analysis of flight bookingsOur company specializes in flight management. Our transactional database stores comprehensive information about airports, airlines, passengers, flights, and bookings. Notably, a passenger can make multiple bookings on the same flight.
The database also contains additional information, such as details about employees, weather, and flight schedules, which are not pertinent to our analysis.

Airflow DAG Overview
This Airflow DAG automates the process of moving data from an OLTP system to an OLAP system using Google Cloud services. Here's how it works:

Setup Default Arguments:
The DAG is initialized with default arguments like owner, start date, retries, and retry delay.

Task Definitions:
Several PythonOperator tasks are defined:
create_table_in_bigquery: Extracts schemas from the OLTP system and creates tables in BigQuery's staging dataset.
export_data_to_bigquery: Transfers data from a MySQL OLTP database to BigQuery.
Facts_and_Dimensions: Creates facts and dimensions tables in the OLAP database.
export_data_from_OLTP_to_OLAP: Cleans data in the staging database and exports it to the OLAP dataset.
DAG Structure:
Tasks are organized with dependencies using the >> operator, ensuring they run sequentially. For instance, create_table_in_bigquery must finish before export_data_to_bigquery starts.

Schedule Interval:
The DAG is scheduled to run every 10 minutes (*/10 * * * *).

Catchup and Max Active Runs:
Parameters like catchup and max_active_runs control Airflow's handling of past runs and the maximum number of concurrent DAG runs.

Tags:
Tags are added to categorize the DAG for organizational purposes.

When triggered, Airflow schedules and executes tasks based on dependencies and schedule intervals. Each task invokes a Python function to perform its data processing task. Airflow logs the status and results of each task, providing visibility into the pipeline's progress and any issues encountered.

This setup ensures a robust and automated data pipeline, facilitating efficient data movement from OLTP to OLAP systems.

The Airflow DAG file responsible for orchestrating this process can be found here
https://github.com/MuhammadGhulamAbbas/Semester-Project-Datawarehousing/blob/main/Pipeline%20Python%20Script%20(Conversion%20Functions)%20(PYT)/main_dags.py
![image](https://github.com/MuhammadGhulamAbbas/Semester-Project-Datawarehousing/assets/83417345/a86ab170-7b3d-4bc0-9c30-e4b92e044a55)
![Image](https://github.com/MuhammadGhulamAbbas/Semester-Project-Datawarehousing/blob/main/IMAGES/WhatsApp%20Image%202024-06-02%20at%2014.35.55%20(2).jpeg)

LIVE TESTING ON POWERBI:-
![Dashboard Image](https://github.com/MuhammadGhulamAbbas/Semester-Project-Datawarehousing/blob/main/Dashboarding%20(PowerBI)/WhatsApp%20Image%202024-06-07%20at%2023.04.27.jpeg)

After Changes
![Dashboard Image 2](https://github.com/MuhammadGhulamAbbas/Semester-Project-Datawarehousing/blob/main/Dashboarding%20(PowerBI)/WhatsApp%20Image%202024-06-01%20at%2000.39.14.jpeg)



