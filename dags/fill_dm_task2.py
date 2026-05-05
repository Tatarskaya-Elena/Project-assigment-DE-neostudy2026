from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import time
import logging
import os

logger = logging.getLogger(__name__)

SQL_PATH = "/opt/airflow/sql/project_sql"

def read_sql_file(filename):
    filepath = os.path.join(SQL_PATH, filename)
    with open(filepath, 'r', encoding='utf-8') as f:
        return f.read()

def log_start_end(task_name, start_time, end_time, status, error_message=None):
    try:
        pg_hook = PostgresHook(postgres_conn_id="Postgres1")
        duration = int((end_time - start_time).total_seconds())
        pg_hook.run("""
            INSERT INTO LOGS.data_load_logs 
            (task_name, start_time, end_time, status, duration_seconds, error_message)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, parameters=[task_name, start_time, end_time, status, duration, error_message])
    except Exception as e:
        logger.error(f"Log error: {e}")

def insert_initial_balance(**context):
    start_time = datetime.now()
    status = "RUNNING"
    error_message = None
    try:
        pg_hook = PostgresHook(postgres_conn_id="Postgres1")
        
        pg_hook.run("DELETE FROM dm.dm_account_balance_f WHERE on_date = '2017-12-31'::DATE;")
        
        sql_select = read_sql_file("insert_balance.sql")
        
        sql_insert = f"""
        INSERT INTO dm.dm_account_balance_f (account_rk, balance_out, balance_out_rub, on_date)
        {sql_select}
        """
        pg_hook.run(sql_insert)
        status = "SUCCESS"
    except Exception as e:
        status = "FAILED"
        error_message = str(e)
        raise
    finally:
        end_time = datetime.now()
        log_start_end("insert_initial_balance", start_time, end_time, status, error_message)

def fill_account_turnover_f(date, **context):
    start_time = datetime.now()
    status = "RUNNING"
    error_message = None
    try:
        pg_hook = PostgresHook(postgres_conn_id="Postgres1")
        
        pg_hook.run(f"DELETE FROM dm.dm_account_turnover_f WHERE on_date = '{date}'::DATE;")
        
        sql_select = read_sql_file("fill_account_turnover_f.sql").format(on_date=date)
        
        sql_insert = f"""
        INSERT INTO dm.dm_account_turnover_f (on_date, account_rk, credit_amount, credit_amount_rub, debet_amount, debet_amount_rub)
        {sql_select}
        """
        pg_hook.run(sql_insert)
        status = "SUCCESS"
    except Exception as e:
        status = "FAILED"
        error_message = str(e)
        raise
    finally:
        end_time = datetime.now()
        log_start_end(f"fill_account_turnover_f_{date}", start_time, end_time, status, error_message)

def fill_account_balance_f(date, **context):
    start_time = datetime.now()
    status = "RUNNING"
    error_message = None
    try:
        pg_hook = PostgresHook(postgres_conn_id="Postgres1")
        
        pg_hook.run(f"DELETE FROM dm.dm_account_balance_f WHERE on_date = '{date}'::DATE;")
        
        sql_select = read_sql_file("fill_account_balance_f.sql").format(on_date=date)
        
        sql_insert = f"""
        INSERT INTO dm.dm_account_balance_f (account_rk, balance_out, balance_out_rub, on_date)
        {sql_select}
        """
        pg_hook.run(sql_insert)
        status = "SUCCESS"
    except Exception as e:
        status = "FAILED"
        error_message = str(e)
        raise
    finally:
        end_time = datetime.now()
        log_start_end(f"fill_account_balance_f_{date}", start_time, end_time, status, error_message)

default_args = {
    "owner": "misha",
    "start_date": datetime(2026, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    "fill_dm",
    default_args=default_args,
    description="Fill DM",
    schedule=None,
    catchup=False,
    tags=["dm"]
) as dag:

    start = EmptyOperator(task_id="start")

    create_showcase = SQLExecuteQueryOperator(
        task_id="create_showcase",
        conn_id="Postgres1",
        sql=read_sql_file("create_showcase.sql")
    )

    insert_balance = PythonOperator(
        task_id="insert_balance",
        python_callable=insert_initial_balance
    )

    turnover_tasks = []
    for day in range(1, 32):
        date = f"2018-01-{day:02d}"
        task = PythonOperator(
            task_id=f"turnover_{date}",
            python_callable=fill_account_turnover_f,
            op_kwargs={"date": date}
        )
        turnover_tasks.append(task)

    balance_tasks = []
    for day in range(1, 32):
        date = f"2018-01-{day:02d}"
        task = PythonOperator(
            task_id=f"balance_{date}",
            python_callable=fill_account_balance_f,
            op_kwargs={"date": date}
        )
        balance_tasks.append(task)

    end = EmptyOperator(task_id="end")

    start >> create_showcase 

    for task in turnover_tasks:
        create_showcase  >> task

    turnover_tasks[-1] >> insert_balance

    prev = insert_balance
    for task in balance_tasks:
        prev >> task
        prev = task

    prev >> end