from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.models import Variable
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

def fill_f101_round_f(date, **context):
    start_time = datetime.now()
    status = "RUNNING"
    error_message = None
    try:
        pg_hook = PostgresHook(postgres_conn_id="Postgres1")
        
        from_date = (datetime.strptime(date, '%Y-%m-%d') - timedelta(days=1)).replace(day=1).strftime('%Y-%m-%d')
        to_date = (datetime.strptime(date, '%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')
        
        pg_hook.run(f"DELETE FROM dm.dm_f101_round_f WHERE from_date = '{from_date}' AND to_date = '{to_date}';")
        
        sql_select = read_sql_file("fill_f101_round_f.sql").format(
            date=date
        )
        
        sql_insert = f"""
        INSERT INTO dm.dm_f101_round_f (
            from_date, to_date, chapter, ledger_account, characteristic,
            balance_in_rub, r_balance_in_rub, balance_in_val, r_balance_in_val,
            balance_in_total, r_balance_in_total, turn_deb_rub, r_turn_deb_rub,
            turn_deb_val, r_turn_deb_val, turn_deb_total, r_turn_deb_total,
            turn_cre_rub, r_turn_cre_rub, turn_cre_val, r_turn_cre_val,
            turn_cre_total, r_turn_cre_total, balance_out_rub, r_balance_out_rub,
            balance_out_val, r_balance_out_val, balance_out_total, r_balance_out_total)
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
        log_start_end(f"fill_f101_round_f_{date}", start_time, end_time, status, error_message)

with DAG(
    "fill_f101",
    description="Fill F101 round form",
    schedule=None,
    catchup=False,
    tags=["dm", "f101"]
) as dag:
    
    start = EmptyOperator(task_id="start")
    
    create_f101_table = SQLExecuteQueryOperator(
        task_id="create_f101_table",
        conn_id="Postgres1",
        sql=read_sql_file("create_f101_table.sql")
    )
    
    calculate_f101 = PythonOperator(
        task_id="calculate_f101",
        python_callable=fill_f101_round_f,
        op_kwargs={"date": "2018-02-01"}
    )

    end = EmptyOperator(task_id="end")
    
    start >> create_f101_table >> calculate_f101 >> end