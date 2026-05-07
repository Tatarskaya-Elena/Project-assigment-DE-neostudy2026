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

def import_f101_from_csv(**context):
    start_time = datetime.now()
    rows_processed = 0
    try:
        pg_hook = PostgresHook(postgres_conn_id="Postgres1")
        pg_hook.run(read_sql_file("create_f101_v2.sql"))
        csv_path = os.path.join(DATA_PATH, "dm_f101_round_f.csv")
        df = pd.read_csv(csv_path, encoding='utf-8-sig')
        rows_processed = len(df)
        if not df.empty:
            from_date = df['from_date'].iloc[0]
            to_date = df['to_date'].iloc[0]
            pg_hook.run(f"DELETE FROM dm.dm_f101_round_f_v2 WHERE from_date = '{from_date}' AND to_date = '{to_date}';")
        engine = pg_hook.get_sqlalchemy_engine()
        df.to_sql("dm_f101_round_f_v2", engine, schema="dm", if_exists="append", index=False)
        end_time = datetime.now()
        log_start_end("import_f101_from_csv", start_time, end_time, "SUCCESS", rows_processed)
    except Exception as e:
        end_time = datetime.now()
        log_start_end("import_f101_from_csv", start_time, end_time, "FAILED", error_message=str(e))
        raise

default_args = {
    "owner": "misha",
    "start_date": datetime(2026, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    "f101_import_from_csv",
    default_args=default_args,
    description="Import F101 from CSV to v2 table",
    schedule=None,
    catchup=False,
    tags=["f101", "import"]
) as dag:
    start = EmptyOperator(task_id="start")
    import_task = PythonOperator(
        task_id="import_f101_from_csv",
        python_callable=import_f101_from_csv
    )
    end = EmptyOperator(task_id="end")
    start >> import_task >> end
