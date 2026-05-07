from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import logging
import os

logger = logging.getLogger(__name__)

SQL_PATH = "/opt/airflow/sql/project_sql"
DATA_PATH = "/opt/airflow/data"

def read_sql_file(filename):
    filepath = os.path.join(SQL_PATH, filename)
    with open(filepath, 'r', encoding='utf-8') as f:
        return f.read()

def log_start_end(task_name, start_time, end_time, status, rows_processed=None, error_message=None):
    try:
        pg_hook = PostgresHook(postgres_conn_id="Postgres1")
        duration = int((end_time - start_time).total_seconds())
        pg_hook.run("""
            INSERT INTO LOGS.data_load_logs 
            (task_name, start_time, end_time, status, duration_seconds, rows_processed, error_message)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, parameters=[task_name, start_time, end_time, status, duration, rows_processed, error_message])
    except Exception as e:
        logger.error(f"Log error: {e}")

def export_f101_to_csv(**context):
    start_time = datetime.now()
    rows_processed = 0
    try:
        pg_hook = PostgresHook(postgres_conn_id="Postgres1")
        sql = read_sql_file("export_f101.sql")
        df = pg_hook.get_pandas_df(sql)
        rows_processed = len(df)
        csv_path = os.path.join(DATA_PATH, "dm_f101_round_f.csv")
        df.to_csv(csv_path, index=False, encoding='utf-8-sig')
        end_time = datetime.now()
        log_start_end("export_f101_to_csv", start_time, end_time, "SUCCESS", rows_processed)
    except Exception as e:
        end_time = datetime.now()
        log_start_end("export_f101_to_csv", start_time, end_time, "FAILED", error_message=str(e))
        raise

default_args = {
    "owner": "misha",
    "start_date": datetime(2026, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    "f101_export_to_csv",
    default_args=default_args,
    description="Export F101 to CSV file",
    schedule=None,
    catchup=False,
    tags=["f101", "export"]
) as dag:
    start = EmptyOperator(task_id="start")
    export = PythonOperator(
        task_id="export_f101_to_csv",
        python_callable=export_f101_to_csv
    )
    end = EmptyOperator(task_id="end")
    start >> export >> end