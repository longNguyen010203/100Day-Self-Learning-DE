from __future__ import annotations

import os
import logging
import pandas as pd
from typing import Any
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator




TABLE_NAME="YELLOW_TAXI_2024_01"
STAGE_NAME="NYC_TAXI_STAGE"
SCHEMA_NAME="NYC_TAXI"
DATABASE_NAME="WAREHOUSE"
SNOWFLAKE_CONN_ID="snowflake_conn_id"
FILE_FORMAT_NAME = "nytaxi_parquet_format"
FILE_PATH="/opt/airflow/data/yellow_tripdata_2024-01.parquet"


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
    dag_id="filesystem_to_snowflake_v100",
    default_args=default_args,
    start_date=datetime(2024, 10, 1),
    schedule_interval="0 23 * * Mon,Wed,Fri",
    tags=["FileSystem", "ETL", "Data Engineer", "Snowflake"],
    catchup=False
)
def etl_pipeline_filesystem_to_snowflake() -> None:
    
    #-------------------------------------------#
    # etl pipeline from Filesystem to Snowflake #
    #-------------------------------------------#
    
    #--------------------------------------------#
    # extract and transform data from filesystem #
    #--------------------------------------------#
    @task(multiple_outputs=True)
    def extract_and_transform_data_from_file(**context) -> dict[str, Any]:
        
        """ Read data from filesystem """
        df = pd.read_parquet(path=FILE_PATH)
        
        """ Transform data """
        # drop duplicates
        df = df.drop_duplicates()
        # drop NaN
        df = df.dropna()
        
        try: os.mkdir("/tmp/transformed")
        except FileExistsError: print("File exists: /tmp/transformed")
        
        tmp_file_path = "/tmp/transformed/file-{}-{}.csv".format(
            datetime.today().strftime("%Y%m%d%H%M%S"),
            "-".join([DATABASE_NAME, SCHEMA_NAME, TABLE_NAME])
        )
        df.to_csv(tmp_file_path, index=False)
        context["ti"].xcom_push(key="tmp_file_path", value=tmp_file_path)
        
        return {
            "Record number": df.shape[0],
            "column number": df.shape[1]
        }
        
    #----------------------------#
    # Create schema at Snowflake #
    #----------------------------#
    create_schema_snowflake = SnowflakeOperator(
        task_id="create_schema_snowflake",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=f"""
            CREATE SCHEMA IF NOT EXISTS {DATABASE_NAME}.{SCHEMA_NAME};
        """
    )
    
    #---------------------------#
    # Create table at Snowflake #
    #---------------------------#
    create_table_snowflake = SnowflakeOperator(
        task_id="create_table_snowflake",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=f"""
            CREATE OR REPLACE TABLE {DATABASE_NAME}.{SCHEMA_NAME}.{TABLE_NAME} (
                VendorID INT,
                tpep_pickup_datetime TIMESTAMP,
                tpep_dropoff_datetime TIMESTAMP,
                passenger_count FLOAT,
                trip_distance FLOAT,
                RatecodeID FLOAT,
                store_and_fwd_flag STRING,
                PULocationID INT,
                DOLocationID INT,
                payment_type BIGINT,
                fare_amount FLOAT,
                extra FLOAT,
                mta_tax FLOAT,
                tip_amount FLOAT,
                tolls_amount FLOAT,
                improvement_surcharge FLOAT,
                total_amount FLOAT,
                congestion_surcharge FLOAT,
                Airport_fee FLOAT
            );
        """
    )
    
    #--------------------#
    # create file format #
    #--------------------#
    create_file_format = SnowflakeOperator(
        task_id="create_file_format",
        snowflake_conn_id="snowflake_conn_id",
        sql=f"""
            USE DATABASE {DATABASE_NAME};
            USE SCHEMA {SCHEMA_NAME};
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
            USE SCHEMA {SCHEMA_NAME};
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
            PUT file://{{ ti.xcom_pull(key='tmp_file_path') }} 
            @{{ params.database_name }}.{{ params.schema_name }}.{{ params.stage_name }};
        """,
        params={
            "database_name": DATABASE_NAME,
            "schema_name": SCHEMA_NAME,
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
            USE SCHEMA {DATABASE_NAME}.{SCHEMA_NAME};
        
            COPY INTO {DATABASE_NAME}.
                      {SCHEMA_NAME}.
                      {TABLE_NAME}
            FROM @{DATABASE_NAME}.{SCHEMA_NAME}.{STAGE_NAME}
            FILE_FORMAT = {DATABASE_NAME}.{SCHEMA_NAME}.{FILE_FORMAT_NAME}
            PURGE = TRUE;
        """,
        params={
            "database_name": DATABASE_NAME,
            "schema_name_snow": SCHEMA_NAME,
            "table_name_snow": TABLE_NAME,
            "stage_name": STAGE_NAME
        }
    )
    

    extract_and_transform_data_from_file() >> put_data_to_stage_in_snowflake
    create_schema_snowflake >> create_table_snowflake >> load_data_into_snowflake_from_file
    create_table_snowflake >> create_stage_table >> put_data_to_stage_in_snowflake
    put_data_to_stage_in_snowflake >> load_data_into_snowflake_from_file
    create_file_format >> put_data_to_stage_in_snowflake
    
etl_pipeline_filesystem_to_snowflake()

    
    
    
    
        
