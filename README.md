# Semester-Project-Datawarehousing

## Data Warehousing Project

### Overview
Implement a data mart providing analysis of flight bookings. Our company specializes in flight management. Our transactional database stores comprehensive information about airports, airlines, passengers, flights, and bookings. Notably, a passenger can make multiple bookings on the same flight. The database also contains additional information, such as details about employees, weather, and flight schedules, which are not pertinent to our analysis.
## Experience Highlights

- Designed ETL processes using Google BigQuery and automated data transformation/loading.
- Leveraged GCP for scalable data storage/processing and created Power BI dashboards for insights.
- Used Python with Pandas and Google Cloud Client for data manipulation/integration.
- Implemented Apache Airflow for workflow automation with Directed Acyclic Graphs (DAGs).
- Encapsulated ETL logic into reusable Python functions, promoting modularity and maintainability.
- Integrated Python, GCP, BigQuery, and Airflow for a robust, scalable data engineering solution.
- Implemented a data mart for flight bookings analysis, enhancing business intelligence capabilities.
- Converted OLTP to OLAP using stored procedures and Airflow for automation, with live testing in Power BI.

## Docker Usage for Apache Airflow

I utilized Docker for managing Apache Airflow, providing a consistent environment across different development and production environments. Here's how you can use Docker with Airflow:

### Using Docker Compose

To simplify the setup process, I used Docker Compose. You can find the `docker-compose.yml` file [here](https://github.com/MuhammadGhulamAbbas/Semester-Project-Datawarehousing/blob/main/docker-compose.yml).

### Building the Docker Image

To build the Docker image for Apache Airflow, you can use the provided Dockerfile. You can find the Dockerfile [here](https://github.com/MuhammadGhulamAbbas/Semester-Project-Datawarehousing/blob/main/dockerfile). Navigate to the directory containing your Dockerfile and run:


### Airflow DAG Overview
This Airflow DAG automates the process of moving data from an OLTP system to an OLAP system using Google Cloud services. Here's how it works:

#### Setup Default Arguments:
The DAG is initialized with default arguments like owner, start date, retries, and retry delay.

#### Task Definitions:
Several PythonOperator tasks are defined:
- **create_table_in_bigquery**: Extracts schemas from the OLTP system and creates tables in BigQuery's staging dataset.
- **export_data_to_bigquery**: Transfers data from a MySQL OLTP database to BigQuery.
- **Facts_and_Dimensions**: Creates facts and dimensions tables in the OLAP database.
- **export_data_from_OLTP_to_OLAP**: Cleans data in the staging database and exports it to the OLAP dataset.

#### DAG Structure:
Tasks are organized with dependencies using the `>>` operator, ensuring they run sequentially. For instance, `create_table_in_bigquery` must finish before `export_data_to_bigquery` starts.

#### Schedule Interval:
The DAG is scheduled to run every 10 minutes (`*/10 * * * *`).

#### Catchup and Max Active Runs:
Parameters like catchup and max_active_runs control Airflow's handling of past runs and the maximum number of concurrent DAG runs.

#### Tags:
Tags are added to categorize the DAG for organizational purposes.

When triggered, Airflow schedules and executes tasks based on dependencies and schedule intervals. Each task invokes a Python function to perform its data processing task. Airflow logs the status and results of each task, providing visibility into the pipeline's progress and any issues encountered.

This setup ensures a robust and automated data pipeline, facilitating efficient data movement from OLTP to OLAP systems.

The Airflow DAG file responsible for orchestrating this process can be found [here](https://github.com/MuhammadGhulamAbbas/Semester-Project-Datawarehousing/blob/main/Pipeline%20Python%20Script%20(Conversion%20Functions)%20(PYT)/main_dags.py).

### Images

#### Airflow DAG:
![Airflow DAG](https://github.com/MuhammadGhulamAbbas/Semester-Project-Datawarehousing/assets/83417345/a86ab170-7b3d-4bc0-9c30-e4b92e044a55)

#### Airflow DAG Structure:
![Airflow DAG Structure](https://github.com/MuhammadGhulamAbbas/Semester-Project-Datawarehousing/blob/main/IMAGES/WhatsApp%20Image%202024-06-02%20at%2014.35.54.jpeg)

#### Data Warehousing:
![Data Warehousing](https://github.com/MuhammadGhulamAbbas/Semester-Project-Datawarehousing/blob/main/IMAGES/WhatsApp%20Image%202024-06-02%20at%2014.35.55%20(2).jpeg)

#### PowerBI Dashboard:
![PowerBI Dashboard](https://github.com/MuhammadGhulamAbbas/Semester-Project-Datawarehousing/blob/main/Dashboarding%20(PowerBI)/WhatsApp%20Image%202024-06-07%20at%2023.04.27.jpeg)

#### Live Testing on PowerBI:
![Live Testing on PowerBI](https://github.com/MuhammadGhulamAbbas/Semester-Project-Datawarehousing/blob/main/Dashboarding%20(PowerBI)/WhatsApp%20Image%202024-06-01%20at%2000.39.14.jpeg)

#### Live Testing on PowerBI:
![Live Testing on PowerBI](https://github.com/MuhammadGhulamAbbas/Semester-Project-Datawarehousing/blob/main/Dashboarding%20(PowerBI)/WhatsApp%20Image%202024-06-01%20at%2000.39.14.jpeg)
