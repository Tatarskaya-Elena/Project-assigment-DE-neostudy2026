from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.models import Variable
import pandas as pd
import time
import logging
import re
import os

logger = logging.getLogger(__name__)

SQL_PATH = "/opt/airflow/sql/project_sql"
DATA_PATH = "/opt/airflow/data"

PK_CONFIG = {
    "ft_balance_f": ["on_date", "account_rk"],
    "ft_posting_f": None,
    "md_account_d": ["data_actual_date", "account_rk"],
    "md_currency_d": ["currency_rk", "data_actual_date"],
    "md_exchange_rate_d": ["data_actual_date", "currency_rk"],
    "md_ledger_account_s": ["ledger_account", "start_date"]
}

def read_sql_file(filename):
    filepath = os.path.join(SQL_PATH, filename)
    with open(filepath, 'r', encoding='utf-8') as f:
        return f.read()

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
                return datetime.strptime(date_str, fmt).date()
            except ValueError:
                continue
    return None

def log_start_end(task_name, table_name, start_time, end_time, status, rows_processed, error_message=None):
    try:
        pg_hook = PostgresHook(postgres_conn_id="Postgres1")
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
        time.sleep(5)
        
        pg_hook = PostgresHook(postgres_conn_id="Postgres1")
        
        pk_columns = PK_CONFIG.get(table_name)
        if pk_columns is None:
            pg_hook.run(f"TRUNCATE TABLE DS.{table_name};")
        
        csv_path = f"{DATA_PATH}/{table_name}.csv"
        encodings = ['utf-8', 'cp1251', 'latin1', 'iso-8859-1', 'cp866']
        df = None
        
        for enc in encodings:
            try:
                df = pd.read_csv(csv_path, delimiter=';', encoding=enc)
                logger.info(f"Read {table_name}.csv with {enc}")
                break
            except UnicodeDecodeError:
                continue
            except Exception as e:
                logger.warning(f"Error with {enc}: {e}")
                continue
        
        if df is None:
            raise Exception(f"Cannot read {csv_path}")
        
        df.columns = df.columns.str.lower()

        if table_name == 'md_currency_d':
            if 'currency_code' in df.columns:
                df['currency_code'] = df['currency_code'].astype(str).str[:3]
            if 'code_iso_char' in df.columns:
                df['code_iso_char'] = df['code_iso_char'].astype(str).str[:3]
        
        date_columns = [col for col in df.columns if 'date' in col.lower()]
        for col in date_columns:
            df[col] = df[col].apply(parse_date)
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.date
        
        rows_processed = len(df)
        
        if pk_columns is None:
            engine = pg_hook.get_sqlalchemy_engine()
            df.to_sql(table_name, engine, schema='ds', if_exists='append', index=False)
        else:
            pg_conn = pg_hook.get_conn()
            cursor = pg_conn.cursor()
            
            for _, row in df.iterrows():
                cols = []
                vals = []
                for col in df.columns:
                    cols.append(col)
                    val = row[col]
                    if val is None or pd.isna(val):
                        vals.append('NULL')
                    elif isinstance(val, (datetime, pd.Timestamp, type(pd.to_datetime('2020-01-01')))):
                        vals.append(f"'{val}'")
                    elif isinstance(val, (int, float)):
                        vals.append(str(val))
                    else:
                        vals.append(f"'{val}'")
                
                cols_str = ', '.join(cols)
                vals_str = ', '.join(vals)
                update_cols = ', '.join([f"{col} = EXCLUDED.{col}" for col in df.columns if col not in pk_columns])
                pk_condition = ', '.join(pk_columns)
                
                sql = f"""
                    INSERT INTO DS.{table_name} ({cols_str})
                    VALUES ({vals_str})
                    ON CONFLICT ({pk_condition})
                    DO UPDATE SET {update_cols}
                """
                cursor.execute(sql)
            
            pg_conn.commit()
            cursor.close()
            pg_conn.close()
        
        status = "SUCCESS"
        logger.info(f"Loaded {rows_processed} rows into {table_name}")
        
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
    "ds_data_load",
    default_args=default_args,
    description="Load banking data from CSV to DS layer",
    schedule=None,
    catchup=False,
    tags=["ds", "etl"]
) as dag:
    
    start = EmptyOperator(task_id="start")
    
    create_schemas = SQLExecuteQueryOperator(
        task_id="create_schemas",
        conn_id="Postgres1",
        sql=read_sql_file("create_schemas.sql")
    )
    
    create_ds_tables = SQLExecuteQueryOperator(
        task_id="create_ds_tables",
        conn_id="Postgres1",
        sql=read_sql_file("create_ds_tables.sql")
    )
    
    create_log_table = SQLExecuteQueryOperator(
        task_id="create_log_table",
        conn_id="Postgres1",
        sql=read_sql_file("create_log_table.sql")
    )
    
    split = EmptyOperator(task_id="split")

    load_ft_balance_f = PythonOperator(
        task_id="load_ft_balance_f",
        python_callable=load_table,
        op_kwargs={"table_name": "ft_balance_f"}
    )
    
    load_ft_posting_f = PythonOperator(
        task_id="load_ft_posting_f",
        python_callable=load_table,
        op_kwargs={"table_name": "ft_posting_f"}
    )
    
    load_md_account_d = PythonOperator(
        task_id="load_md_account_d",
        python_callable=load_table,
        op_kwargs={"table_name": "md_account_d"}
    )
    
    load_md_currency_d = PythonOperator(
        task_id="load_md_currency_d",
        python_callable=load_table,
        op_kwargs={"table_name": "md_currency_d"}
    )
    
    load_md_exchange_rate_d = PythonOperator(
        task_id="load_md_exchange_rate_d",
        python_callable=load_table,
        op_kwargs={"table_name": "md_exchange_rate_d"}
    )
    
    load_md_ledger_account_s = PythonOperator(
        task_id="load_md_ledger_account_s",
        python_callable=load_table,
        op_kwargs={"table_name": "md_ledger_account_s"}
    )
    
    all_loads_done = EmptyOperator(task_id="all_loads_done", trigger_rule="all_success")
    end = EmptyOperator(task_id="end")
    
    start >> create_schemas >> [create_ds_tables, create_log_table] >> split >> [
        load_ft_balance_f, load_ft_posting_f, load_md_account_d,
        load_md_currency_d, load_md_exchange_rate_d, load_md_ledger_account_s
    ] >> all_loads_done >> end
