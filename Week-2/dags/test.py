from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator



with DAG(dag_id="test_airflow",
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
         start_date=datetime(2024, 6, 26),
         schedule_interval='0 0 * * *',
         catchup=False
) as dag:
    
    test_apache_airflow = BashOperator(
        task_id="test_apache_airflow",
        bash_command="echo Hello World"
    )