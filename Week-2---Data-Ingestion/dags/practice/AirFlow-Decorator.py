from __future__ import annotations

from typing import Any
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator




@dag(
    dag_id="de_xcom_practice_v17",
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
    tags=["TaskFlow", "traditional operators"],
    catchup=True
)
def standard_xcom_taskflow_traditional() -> None:
    
    #------------------------------------------#
    # Traditional to TaskFlow, implicit syntax #
    #------------------------------------------#
    
    bash_task_traditinonal = BashOperator(
        task_id="bash_task_traditinonal",
        bash_command="echo {{ ti.xcom_push(key='country_name', value='NYC') }}"
    )
    
    @task(
        multiple_outputs=True
    )
    def sender_task_to_xcom(**context) -> str:
        # push values to XCom explicitly with a specific key by using the .xcom_push method
        # of the task instance (ti) in the airflow context
        context["ti"].xcom_push(key="my_number", value=21)
        context["ti"].xcom_push(key="birthYear", value=1995)
        
        # the return value gets pushed to XCom implicitly (with the key 'return_value')
        return {
            "name_1": "Justin Bieber",
            "name_2": "Billie Eilish"
        }
    
    @task(
        templates_dict={
            "received": "{{ ti.xcom_pull(key='country_name') }}"
        }
    )
    def receiver_task(xcom_received, **context) -> None:
        # pull values from XCom explicitly with a specific key by using the .xcom_pull methed
        my_number = context["ti"].xcom_pull(key="my_number")
        birthYear = context["ti"].xcom_pull(key="birthYear")
        country_name = context["templates_dict"]["received"]
        
        print(xcom_received["name_1"] + f" {my_number} years old!")
        print(xcom_received["name_2"] + f" {birthYear} years!")
        print(xcom_received["name_1"] + f" {country_name} city!")
        
    # pass the return value of one task to the next one to implicitly use XCom
    bash_task_traditinonal >> receiver_task(sender_task_to_xcom())

standard_xcom_taskflow_traditional()