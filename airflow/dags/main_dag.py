import airflow
from airflow import DAG
#from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.sqlite_operator import SqliteOperator
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os
from dotenv import load_dotenv
from functions import clickhouse_reader
load_dotenv()

default_args = {
            "owner": "Dammy",
            "start_date": airflow.utils.dates.days_ago(1),
            "depends_on_past": False,
            "email_on_failure": False,
            "email_on_retry": False,
            "email": os.environ['email'],
            "retries": 1,
            "retry_delay": timedelta(minutes=5)
        }



# define path where insert script will be kept
sql_script_path = clickhouse_reader.dir + '/dags/queries/sql_insert_query.sql' 


def read_query():
    with open(sql_script_path,mode='r') as f:
        global query
        query = f.read()
    return query

def clickhouse_extractor():
    clickhouse_reader.read_clickhouse()

with DAG(dag_id="CLICKHOUSE-SQL-ETL", schedule_interval=None, 
    default_args=default_args, catchup=False) as dag:

    #extract data from clickhouse - outputs an sqlite script for the next task
    extract_data = PythonOperator(
        task_id = "extract_data",
        python_callable = clickhouse_extractor
    )

    create_table = SqliteOperator(
        task_id = 'create_table',
        sql = """
        CREATE TABLE IF NOT EXISTS trips (
        MONTH DATE, sat_mean_trip_count REAL, sat_mean_fare_trip REAL, sat_mean_duration_per_trip_mins REAL, 
        sun_mean_trip_count REAL, sun_mean_fare_trip REAL,sun_mean_duration_per_trip_mins REAL); 
        \n\n""",
        sqlite_conn_id = 'airflow_sqlite_db'
    )

    delete_existing_records = SqliteOperator(
        task_id = 'delete_existing_records',
        sql = "DELETE FROM trips",
        sqlite_conn_id = 'airflow_sqlite_db'
    )

    load_query = PythonOperator(
        task_id = "load_query",
        python_callable = read_query
    )

    load_data_sql = SqliteOperator(
        task_id = 'load_table',
        sql = "{{ task_instance.xcom_pull(task_ids='load_query')}}",
        sqlite_conn_id = 'airflow_sqlite_db'
    )

extract_data >> create_table >> delete_existing_records >> load_query >> load_data_sql