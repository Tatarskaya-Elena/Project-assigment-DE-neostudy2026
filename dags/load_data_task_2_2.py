from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.models import Variable
import pandas as pd
import logging
import re
import os

logger = logging.getLogger(__name__)

SQL_PATH = "/opt/airflow/sql/project_sql"
DATA_PATH = "/opt/airflow/data"

def read_sql_file(filename):
    filepath = os.path.join(SQL_PATH, filename)
    with open(filepath, 'r', encoding='utf-8') as f:
        return f.read()


def read_csv_file(filepath):
    encodings = ['utf-8', 'cp1251', 'latin1', 'iso-8859-1', 'cp866']
    for enc in encodings:
        try:
            df = pd.read_csv(filepath, delimiter=',', encoding=enc)
            logger.info(f"Read {filepath} with {enc}")
            return df
        except UnicodeDecodeError:
            continue
        except Exception as e:
            logger.warning(f"Error with {enc}: {e}")
            continue
    raise Exception(f"Cannot read {filepath}")

def parse_date(value):
    if pd.isna(value) or value is None or str(value).strip() == '':
        return None
    date_str = str(value).strip()
    patterns = [
        (r'^\d{2}-\d{2}-\d{4}$', '%d-%m-%Y'),
        (r'^\d{4}-\d{2}-\d{2}$', '%Y-%m-%d'),
        (r'^\d{2}\.\d{2}\.\d{4}$', '%d.%m.%Y')
    ]
    for pattern, fmt in patterns:
        if re.match(pattern, date_str):
            try:
                result = datetime.strptime(date_str, fmt).date()
                return result
            except ValueError:
                continue
    return None

def log_start_end(task_name, table_name, start_time, end_time, status, rows_processed, error_message=None):
    try:
        pg_hook = PostgresHook(postgres_conn_id="dwh")
        duration = int((end_time - start_time).total_seconds()) if end_time and start_time else None
        pg_hook.run("""
            INSERT INTO LOGS.data_load_logs 
            (task_name, start_time, end_time, status, rows_processed, error_message, duration_seconds)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, parameters=[task_name, start_time, end_time, status, rows_processed, error_message, duration])
    except Exception as e:
        logger.error(f"Log error: {e}")

def load_table(table_name, **context):
    start_time = datetime.now()
    rows_processed = 0
    status = "RUNNING"
    error_message = None
    
    try:
        logger.info(f"Loading {table_name}")
        
        pg_hook = PostgresHook(postgres_conn_id="dwh")
        
        csv_filename = table_name if '_info' in table_name else f"{table_name}_info"

        csv_path = f"{DATA_PATH}/{csv_filename}.csv"
        df = read_csv_file(csv_path)
        
        df.columns = df.columns.str.lower()
        
        date_columns = [col for col in df.columns if 'date' in col.lower()]
        for col in date_columns:
            df[col] = df[col].apply(parse_date)

            #df[col] = pd.to_datetime(df[col], errors='coerce').dt.date
        
        rows_processed = len(df)
        
        if table_name == 'product':
            pg_hook.run(f"TRUNCATE TABLE rd.{table_name};")
            
            engine = pg_hook.get_sqlalchemy_engine()
            df.to_sql(table_name, engine, schema='rd', if_exists='append', index=False)
            logger.info(f"Truncated and loaded {rows_processed} rows into RD.{table_name}")
            
        elif table_name == 'deal_info':
            distinct_dates = df['effective_from_date'].unique()
            
            for load_date in distinct_dates:
                pg_hook.run(f"""
                    DELETE FROM rd.{table_name} 
                    WHERE effective_from_date = '{load_date}'
                """)
                logger.info(f"Deleted existing records for date {load_date}")
            
            engine = pg_hook.get_sqlalchemy_engine()
            df.to_sql(table_name, engine, schema='rd', if_exists='append', index=False)
            logger.info(f"Loaded {rows_processed} rows into RD.{table_name} for dates: {distinct_dates}")
        
        status = "SUCCESS"
        
    except Exception as e:
        status = "FAILED"
        error_message = str(e)
        logger.error(f"Error loading {table_name}: {error_message}")
        raise
        
    finally:
        end_time = datetime.now()
        log_start_end("load_table", table_name, start_time, end_time, status, rows_processed, error_message)

default_args = {
    "owner": "misha",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    "rd_data_load",
    default_args=default_args,
    description="Load product and deal_info data from CSV to RD layer",
    schedule=None,
    catchup=False,
    tags=["rd", "etl"]
) as dag:
    
    start = EmptyOperator(task_id="start")

    create_log_schema = SQLExecuteQueryOperator(
        task_id="create_log_schema",
        conn_id="dwh",
        sql= "CREATE SCHEMA IF NOT EXISTS LOGS;"
    )
    
    create_log_table = SQLExecuteQueryOperator(
        task_id="create_log_table",
        conn_id="dwh",
        sql=read_sql_file("create_log_table.sql")
    )
    
    load_product = PythonOperator(
        task_id="load_product",
        python_callable=load_table,
        op_kwargs={"table_name": "product"}
    )
    
    load_deal_info = PythonOperator(
        task_id="load_deal_info",
        python_callable=load_table,
        op_kwargs={"table_name": "deal_info"}
    )
    
    end = EmptyOperator(task_id="end")
    
    start >> create_log_schema >> create_log_table >> [load_product, load_deal_info] >> end
