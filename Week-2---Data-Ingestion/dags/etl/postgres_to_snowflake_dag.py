from __future__ import annotations

import os
import pandas as pd
from typing import Any
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook




default_args = {
    'owner': 'longdata',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'email': ['myemail@domain.com']
}

DATABASE_NAME_POSTGRES="dvdrental"
SCHEMA_NAME="mart"
TABLE_NAME="dim_CustomerInforDetail"
EXTRACT_DATA_POSTGRES=f"""SELECT * FROM {SCHEMA_NAME}."{TABLE_NAME}" LIMIT 100;"""

TABLE_NAME_SNOW="CUSTOMER_DETAIL"
DATABASE_NAME="DVDRENTAL"
SCHEMA_NAME_SNOW="STAGING"
SNOWFLAKE_CONN_ID="snowflake_conn_id"


@dag(
    dag_id="postgres_to_snowflake_v43",
    default_args=default_args,
    start_date=datetime(2024, 10, 1),
    schedule_interval="0 23 * * Mon,Wed,Fri",
    tags=["TaskFlow", "ETL", "Data Engineer"],
    catchup=False
)
def etl_pipeline_postgres_to_snowflake() -> None:

    #-----------------------------------------#
    # etl pipeline from Postgres to Snowflake #
    #-----------------------------------------#
    
    #----------------------------#
    # Create schema at Snowflake #
    #----------------------------#
    create_schema_snowflake = SnowflakeOperator(
        task_id="create_schema_snowflake",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=f"""
            CREATE SCHEMA IF NOT EXISTS {DATABASE_NAME}.{SCHEMA_NAME_SNOW};
        """
    )
    
    #---------------------------#
    # Create table at Snowflake #
    #---------------------------#
    create_table_snowflake = SnowflakeOperator(
        task_id="create_table_snowflake",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=f"""
            CREATE OR REPLACE TABLE {DATABASE_NAME}.{SCHEMA_NAME_SNOW}.{TABLE_NAME_SNOW} (
                customer_id INT NOT NULL,
                customer_fullname STRING NOT NULL,
                email_of_customer VARCHAR(50) NOT NULL,
                address_of_customer VARCHAR(50) NOT NULL,
                active_bool BOOLEAN NOT NULL,
                create_date DATE NOT NULL,
                phone_of_customer VARCHAR(20) NOT NULL,
                district VARCHAR(20) NOT NULL,
                postal_code VARCHAR(10) NOT NULL,
                city_customer VARCHAR(50) NOT NULL,
                country_customer VARCHAR(50) NOT NULL,
                primary key (customer_id)
            );
        """
    )

    #------------------------------#
    # extract data from PostgreSQL #
    #------------------------------#
    @task(multiple_outputs=True)
    def extract_data_from_postgres(**context) -> dict[str, Any]:
        """ Establish a connection to a postgres database. """
        
        # Connect to PostgreSQL
        hook = PostgresHook(
            postgres_conn_id="postgres_conn_id",
            enable_log_db_messages=True
        )
        
        # Execute the query
        connection = hook.get_conn()
        cursor = connection.cursor()
        cursor.execute(query=EXTRACT_DATA_POSTGRES)
        
        # Extract data
        data = cursor.fetchall()
        cols = [desc[0] for desc in cursor.description]
        
        # Convert data to Dataframe and save data to CSV file
        df = pd.DataFrame(data=data, columns=cols)
        
        try: os.mkdir("/tmp/raw")
        except FileExistsError: print("File exists: /tmp/raw")
        
        tmp_file_path = "/tmp/raw/file-{}-{}.csv".format(
            datetime.today().strftime("%Y%m%d%H%M%S"),
            "-".join([DATABASE_NAME, SCHEMA_NAME_SNOW, TABLE_NAME_SNOW])
        )
        df.to_csv(tmp_file_path, index=False)
        context["ti"].xcom_push(key="tmp_file_path", value=tmp_file_path)
        
        return {
            "Record number": df.shape[0],
            "column number": df.shape[1]
        }
        
    #----------------------------------------------#
    # transform the data extracted from PostgreSQL #
    #----------------------------------------------#
    @task(
        templates_dict={
            "received": "{{ ti.xcom_pull(key='tmp_file_path') }}"
        },
        multiple_outputs=True,
    )
    def transform_data_extracted(**context) -> dict[str, Any]:
        path = context["templates_dict"]["received"]
        df = pd.read_csv(filepath_or_buffer=path)
        
        # drop columns
        df = df.drop(
            columns=["customer_key",
                     "address_of_customer2",
                     "active_number"]
        )
        # drop duplicates
        df = df.drop_duplicates()
        # drop NaN
        df = df.dropna()
        
        try: os.mkdir("/tmp/transformed")
        except FileExistsError: print("File exists: /tmp/transformed")
        
        tmp_file_path = "/tmp/transformed/file-{}-{}.csv".format(
            datetime.today().strftime("%Y%m%d%H%M%S"),
            "-".join([DATABASE_NAME, SCHEMA_NAME_SNOW, TABLE_NAME_SNOW])
        )
        df.to_csv(tmp_file_path, index=False)
        context["ti"].xcom_push(key="tmp_file_path2", value=tmp_file_path)
        
        return {
            "Record number": df.shape[0],
            "column number": df.shape[1]
        }
        
    
    # @task()
    # def load_data_into_snowflake() -> None:
    #     """ Establish a connection to a Snowflake data warehouse. """
        
    #     hook = SnowflakeHook(snowflake_conn_id="snowflake_conn_id")
    #     connect = hook.get_conn()
    #     cursor = connect.cursor()
    #     cursor.execute()
    
    
    #------------------------------------#
    # Load data transformed to Snowflake #
    #------------------------------------#
    put_data_to_stage_in_snowflake = SnowflakeOperator(
        task_id="put_data_to_stage_in_snowflake",
        snowflake_conn_id="snowflake_conn_id",
        sql="""
            PUT file://{{ ti.xcom_pull(key='tmp_file_path2') }} 
            @{{ params.database_name }}.{{ params.schema_name_snow }}.{{ params.table_name_snow }};
        """,
        params={
            "database_name": DATABASE_NAME,
            "schema_name_snow": SCHEMA_NAME_SNOW,
            "table_name_snow": TABLE_NAME_SNOW
        }
    )
    
    #------------------------------------#
    # Load data transformed to Snowflake #
    #------------------------------------#
    load_data_into_snowflake_from_file = SnowflakeOperator(
        task_id="load_data_into_snowflake_from_file",
        snowflake_conn_id="snowflake_conn_id",
        sql="""
            COPY INTO {{ params.database_name }}.
                      {{ params.schema_name_snow }}.
                      {{ params.table_name_snow }}
            FROM @{{ ti.xcom_pull(key='tmp_file_path2') }}
            FILE_FORMAT = (
                TYPE = 'CSV', 
                FIELD_OPTIONALLY_ENCLOSED_BY='"', 
                SKIP_HEADER=1
            )
            VALIDATION_MODE = RETURN_ALL_ERRORS; """,
        params={
            "database_name": DATABASE_NAME,
            "schema_name_snow": SCHEMA_NAME_SNOW,
            "table_name_snow": TABLE_NAME_SNOW
        }
    )
    
    
    create_schema_snowflake >> create_table_snowflake >> load_data_into_snowflake_from_file
    extract_data_from_postgres() >> transform_data_extracted() >> load_data_into_snowflake_from_file
    create_table_snowflake >> put_data_to_stage_in_snowflake
    put_data_to_stage_in_snowflake >> load_data_into_snowflake_from_file
     
etl_pipeline_postgres_to_snowflake()