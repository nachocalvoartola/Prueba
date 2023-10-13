#IMPORT LIBRARIES
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

#PARAMETERS
TAXI_JANUARY_2023_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet"

#DAG DEFINITION
with DAG(
    dag_id = "pipeline",
    catchup = False,
    schedule_interval = "0 1 * * *",
    start_date = datetime(2023, 1, 1)
    ):
    BashOperator(task_id = "download_parquet_taxi", bash_command=f"wget {TAXI_JANUARY_2023_URL}")
