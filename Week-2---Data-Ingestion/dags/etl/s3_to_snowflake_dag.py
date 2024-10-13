from __future__ import annotations

import os
import pandas as pd
from typing import Any
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator



S3_KEY="OnlineRetail.csv"
S3_BUCKET_NAME="online-retail-sale"

TABLE_NAME="ONLINE_RETAIL"
STAGE_NAME="RETAIL_STAGE"
SCHEMA_NAME="SALE"
DATABASE_NAME="WAREHOUSE"
SNOWFLAKE_CONN_ID="snowflake_conn_id"
FILE_FORMAT_NAME = "online_retail_format"


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
    dag_id="s3_to_snowflake_v101",
    default_args=default_args,
    start_date=datetime(2024, 10, 1),
    schedule_interval="0 23 * * Mon,Wed,Fri",
    tags=["S3", "ETL", "Data Engineer", "Snowflake"],
    catchup=False
)
def etl_pipeline_s3_to_snowflake() -> None:
    
    #-----------------------------------#
    # etl pipeline from s3 to Snowflake #
    #-----------------------------------#
    
    #------------------------------#
    # download file from s3 bucket #
    #------------------------------#
    @task()
    def download_file_from_s3_bucket(**context) -> None:
        """ Establish a connection to a s3 bucket. """
        
        # Connect to Amazon S3
        hook = S3Hook(aws_conn_id="amazon_conn_id")
        
        # create directory if not exists
        try: os.mkdir("/tmp/raw")
        except FileExistsError: print("File exists: /tmp/raw")
        
        tmp_file_path = hook.download_file(
            key=S3_KEY,
            bucket_name=S3_BUCKET_NAME,
            local_path="/tmp/raw",
            preserve_file_name=True
        )
        
        context["ti"].xcom_push(key="tmp_file_path", value=tmp_file_path)
        
        
    #--------------------------------------#
    # transform the data extracted from S3 #
    #--------------------------------------#
    @task(
        templates_dict={
            "received": "{{ ti.xcom_pull(key='tmp_file_path') }}"
        },
        multiple_outputs=True,
    )
    def transform_data_extracted(**context) -> dict[str, Any]:
        path = context["templates_dict"]["received"]
        df = pd.read_csv(filepath_or_buffer=path, encoding='latin1')
        
        #rename columns
        df.rename(
            columns={
                'InvoiceNo': 'OrderID', 
                'StockCode': 'ProductID', 
                'InvoiceDate': 'OrderDate'
            }, 
            inplace=True
        )
        
        # change datatype
        df['Quantity'] = df['Quantity'].astype('int32')
        df['UnitPrice'] = df['UnitPrice'].astype('float32')
        df['CustomerID'] = df['CustomerID'].astype('float32')
        df['OrderDate'] = df['OrderDate'].astype('datetime64[ns]')
        
        # drop duplicates
        df = df.drop_duplicates()
        # drop NaN
        df = df.dropna()
        
        try: os.mkdir("/tmp/transformed")
        except FileExistsError: print("File exists: /tmp/transformed")
        
        tmp_file_path = "/tmp/transformed/file-{}-{}".format(
            datetime.today().strftime("%Y%m%d%H%M%S"),
            "-".join([S3_BUCKET_NAME, S3_KEY])
        )
        df.to_csv(tmp_file_path, index=False)
        
        context["ti"].xcom_push(key="tmp_file_path2", value=tmp_file_path)
        
        
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
                OrderID STRING,
                ProductID STRING,
                Description STRING,
                Quantity INT,
                OrderDate TIMESTAMP,
                UnitPrice FLOAT,
                CustomerID FLOAT,
                Country STRING
            );
        """
    )
        
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
            PUT file://{{ ti.xcom_pull(key='tmp_file_path2') }} 
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
    
    
    download_file_from_s3_bucket() >> transform_data_extracted() >> put_data_to_stage_in_snowflake
    create_schema_snowflake >> create_table_snowflake >> load_data_into_snowflake_from_file
    create_table_snowflake >> create_stage_table >> put_data_to_stage_in_snowflake
    put_data_to_stage_in_snowflake >> load_data_into_snowflake_from_file
    create_file_format >> put_data_to_stage_in_snowflake
    
etl_pipeline_s3_to_snowflake()