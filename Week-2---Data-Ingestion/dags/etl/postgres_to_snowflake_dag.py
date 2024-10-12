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




DATABASE_NAME_POSTGRES="dvdrental"
SCHEMA_NAME="mart"
TABLE_NAME="dim_CustomerInforDetail"
EXTRACT_DATA_POSTGRES=f"""SELECT * FROM {SCHEMA_NAME}."{TABLE_NAME}" LIMIT 100;"""

TABLE_NAME_SNOW="CUSTOMER_DETAIL"
DATABASE_NAME="WAREHOUSE"
STAGE_NAME="DVD_STAGE"
SCHEMA_NAME_SNOW="ECOMMERCE"
FILE_FORMAT_NAME="dvd_format"
SNOWFLAKE_CONN_ID="snowflake_conn_id"


# Define the DAG
default_args = {
    'owner': 'longdata',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'email': ['myemail@domain.com']
}


@dag(
    dag_id="postgres_to_snowflake_v66",
    default_args=default_args,
    start_date=datetime(2024, 10, 1),
    schedule_interval="0 23 * * Mon,Wed,Fri",
    tags=["PostgreSQL", "ETL", "Data Engineer", "Snowflake"],
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
                customer_id INT,
                customer_fullname STRING(256),
                email_of_customer STRING(256),
                address_of_customer STRING(256),
                active_bool BOOLEAN,
                create_date DATE,
                phone_of_customer STRING(20),
                district STRING(20),
                postal_code STRING(10),
                city_customer STRING(50),
                country_customer STRING(50)
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
        context["ti"].xcom_push(key="tmp_file_path3", value=tmp_file_path[17:])
        
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
    
    
    #--------------------#
    # create file format #
    #--------------------#
    create_file_format = SnowflakeOperator(
        task_id="create_file_format",
        snowflake_conn_id="snowflake_conn_id",
        sql=f"""
            USE DATABASE {DATABASE_NAME};
            USE SCHEMA {SCHEMA_NAME_SNOW};
            CREATE OR REPLACE FILE FORMAT {FILE_FORMAT_NAME} 
            TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1;
        """
    )
    
    #--------------------#
    # create stage table #
    #--------------------#
    create_stage_table = SnowflakeOperator(
        task_id="create_stage_table",
        snowflake_conn_id="snowflake_conn_id",
        sql=f"""
            USE DATABASE {DATABASE_NAME};
            USE SCHEMA {SCHEMA_NAME_SNOW};
            CREATE OR REPLACE STAGE {STAGE_NAME};
        """
    )
    
    #--------------------------------#
    # put data file into stage table #
    #--------------------------------#
    put_data_to_stage_in_snowflake = SnowflakeOperator(
        task_id="put_data_to_stage_in_snowflake",
        snowflake_conn_id="snowflake_conn_id",
        sql="""
            PUT file://{{ ti.xcom_pull(key='tmp_file_path2') }} 
            @{{ params.database_name }}.{{ params.schema_name_snow }}.{{ params.stage_name }};
        """,
        params={
            "database_name": DATABASE_NAME,
            "schema_name_snow": SCHEMA_NAME_SNOW,
            "stage_name": STAGE_NAME
        }
    )
    
    #------------------------------------#
    # Load data transformed to Snowflake #
    #------------------------------------#
    load_data_into_snowflake_from_file = SnowflakeOperator(
        task_id="load_data_into_snowflake_from_file",
        snowflake_conn_id="snowflake_conn_id",
        sql=f"""
            USE SCHEMA {DATABASE_NAME}.{SCHEMA_NAME_SNOW};
        
            COPY INTO {DATABASE_NAME}.
                      {SCHEMA_NAME_SNOW}.
                      {TABLE_NAME_SNOW}
            FROM @{DATABASE_NAME}.{SCHEMA_NAME_SNOW}.{STAGE_NAME}
            FILE_FORMAT = {DATABASE_NAME}.{SCHEMA_NAME_SNOW}.{FILE_FORMAT_NAME}
            PURGE = TRUE;
        """,
        params={
            "database_name": DATABASE_NAME,
            "schema_name_snow": SCHEMA_NAME_SNOW,
            "table_name_snow": TABLE_NAME_SNOW,
            "stage_name": STAGE_NAME
        }
    )
    
    
    create_schema_snowflake >> create_table_snowflake >> load_data_into_snowflake_from_file
    extract_data_from_postgres() >> transform_data_extracted() >> put_data_to_stage_in_snowflake
    create_table_snowflake >> create_stage_table >> put_data_to_stage_in_snowflake
    put_data_to_stage_in_snowflake >> load_data_into_snowflake_from_file
    create_file_format >> put_data_to_stage_in_snowflake
     
etl_pipeline_postgres_to_snowflake()