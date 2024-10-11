from __future__ import annotations

from typing import Any
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator



@dag(
    dag_id="postgresql_de_v07",
    default_args={
        'owner': 'longdata',
        'retries': 3,
        'retry_delay': timedelta(minutes=5),
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'email': ['myemail@domain.com']
    },
    start_date=datetime(2024, 9, 27),
    schedule_interval="0 23 * * Mon,Wed,Fri",
    tags=["TaskFlow", "traditional operators", "PostgreSQL"],
    catchup=True
)
def dags_with_postgres_operator() -> None:
    
    #-------------------------------#
    # DAGs with PostgreSQL Operator #
    #-------------------------------#
    
    create_postgres_table = PostgresOperator(
        task_id="create_postgres_table",
        postgres_conn_id="postgres_conn_id",
        database="postgres",
        sql=""" 
            CREATE TABLE IF NOT EXISTS public.stocktrade (
                id SERIAL,
                stock_id VARCHAR(5),
                stock_type VARCHAR(5), 
                price DOUBLE PRECISION,
                quantity DOUBLE PRECISION,
                PRIMARY KEY (id)
            );
        """
    )
    
    insert_data_to_postgres = PostgresOperator(
        task_id="insert_data_to_postgres",
        postgres_conn_id="postgres_conn_id",
        database="postgres",
        sql="""
            INSERT INTO public.stocktrade (stock_id, stock_type, price, quantity) 
            VALUES ('AAPL', 'BUY', 175.50, 100),
                   ('GOOG', 'SELL', 2725.80, 50),
                   ('TSLA', 'SELL', 725.50, 200),
                   ('AMZN', 'BUY', 3450.60, 30),
                   ('MSFT', 'SELL', 299.50, 150);
        """
    )
    
    create_postgres_table >> insert_data_to_postgres

dags_with_postgres_operator()