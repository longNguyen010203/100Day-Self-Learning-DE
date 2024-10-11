from __future__ import annotations

from typing import Any
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


## Python function
def greet(names, ages, years) -> None:
    for name, age, year in zip(names, ages, years):
        print(f"{name} - {age} - {year}")
        
def get_accessKey(ti) -> None:
    ti.xcom_push(key="accessKey", value="SCVJOSIISECEZXCXXXJNCIEW")

def login(ti) -> None:
    accessKey = ti.xcom_pull(task_ids="get_accessKey", key="accessKey")
    print(f"Information Login have AccessKey ID: {accessKey}")


with DAG(dag_id="DE_airflow_practice_v08",
         default_args={
             'owner': 'longdata',
             'retries': 3,
             'retry_delay': timedelta(minutes=5),
             'depends_on_past': False,
             'email_on_failure': False,
             'email_on_retry': False,
             'email': ['myemail@domain.com']
         },
         description="""
              Utilize the AWS cloud computing platform 
              to process, calculate, and aggregate large 
              amounts of data, and automate and schedule 
              tasks using Apache Airflow.
            """,
         start_date=datetime(2024, 10, 6),
         schedule_interval="@daily",
         catchup=False
) as dag:
    
    task_1 = BashOperator(
        task_id="task_1",
        bash_command="echo https://longnguyen010203.github.io/"
    )
    
    task_2 = BashOperator(
        task_id="task_2",
        bash_command="echo https://github.com/longNguyen010203/"
    )
    
    task_3 = BashOperator(
        task_id="task_3",
        bash_command="echo https://facebook.com/"
    )
    
    task_4 = PythonOperator(
        task_id="task_4",
        python_callable=greet,
        op_kwargs={
            "names": ["LONG", "THANH", "KHANH", "QUYNH"],
            "ages": [21, 17, 9, 24],
            "years": [2003, 2008, 2015, 2000]
        }
    )
    
    task_5 = PythonOperator(
        task_id="task_5",
        python_callable=get_accessKey
    )
    
    task_6 = PythonOperator(
        task_id="task_6",
        python_callable=login
    )
    
    
    task_1 >> [task_2, task_3]
    task_1 >> [task_4, task_5] >> task_6