from __future__ import annotations

import os
import logging
import pandas as pd
import polars as pl
from typing import Any
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from Operators.SpotifyApiOperator import SpotifyApiOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from dotenv import load_dotenv
load_dotenv()




TABLE_NAME="ARTIST"
DATABASE_NAME="WAREHOUSE"
STAGE_NAME="ARTIST_STAGE"
SCHEMA_NAME="SPOTIFY"
FILE_FORMAT_NAME="artist_csv_format"
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
    dag_id="spotifyApi_to_snowflake_v101",
    default_args=default_args,
    start_date=datetime(2024, 10, 1),
    schedule_interval="0 23 * * Mon,Wed,Fri",
    tags=["SpotifyApi", "ETL", "Data Engineer", "Snowflake"],
    catchup=False
)
def etl_pipeline_spotifyApi_to_snowflake() -> None:

    #--------------------------------------------#
    # etl pipeline from Spotify api to Snowflake #
    #--------------------------------------------#
    
    #-------------------------------#
    # Extract data from Spotify api #
    #-------------------------------#
    @task(multiple_outputs=True)
    def extract_data_from_spotify_api(**context) -> dict[str, Any]:
        spotify = SpotifyApiOperator(
            task_id="connect_spotify_api",
            spotify_client_id=os.getenv("SPOTIFY_CLIENT_ID"),
            spotify_client_secret=os.getenv("SPOTIFY_CLIENT_SECRET")
        )
        
        artist_datas: list[pl.DataFrame] = []
        limit, offset, total_retrieved = 50, 0, 0
        
        while offset <= 900:
            artist_json = spotify.search_for_item(
                limit=limit,
                offset=offset,
                type="artist",
                market="ES"
            )

            try:
                for item in artist_json["artists"]["items"]:
                    genress = [str(genres) for genres in item["genres"]]
                    
                    artist_data = pl.DataFrame(
                        {
                            "personal_page": item["external_urls"]["spotify"],
                            "followers": item["followers"]["total"],
                            "genres": ", ".join(genress),
                            "id": item["id"],
                            "name": item["name"],
                            "popularity": item["popularity"],
                            "type": item["type"]
                        },
                    )
                    artist_datas.append(artist_data)
                    
                total_retrieved += len(artist_json["artists"]["items"])
                offset += limit
                logging.info(f"INFO: offset = {offset}")
                    
            except ValueError as e:
                print(f"Error processing artist data: {e}")
                break
            
        if artist_datas: artist_df: pl.DataFrame = pl.concat(artist_datas)
        else: artist_df: pl.DataFrame = pl.DataFrame()
        artist_df_pd: pd.DataFrame = artist_df.to_pandas()
        
        try: os.mkdir("/tmp/raw")
        except FileExistsError: print("File exists: /tmp/raw")
        
        tmp_file_path = "/tmp/raw/file-{}-{}.csv".format(
            datetime.today().strftime("%Y%m%d%H%M%S"),
            "-".join(["spotify", "artist", "api"])
        )
        
        artist_df_pd.to_csv(tmp_file_path, index=False)
        context["ti"].xcom_push(key="tmp_file_path", value=tmp_file_path)
        
        return {
            "Total artists retrieved": total_retrieved,
            "Record number": artist_df_pd.shape[0],
            "column number": artist_df_pd.shape[1],
            "File path": tmp_file_path
        }
        
        
    #---------------------------------#
    # Transform data from Spotify api #
    #---------------------------------#
    @task(
        templates_dict={
            "received": "{{ ti.xcom_pull(key='tmp_file_path') }}"
        },
        multiple_outputs=True
    )
    def transform_data_from_spotify_api(**context) -> dict[str, Any]:
        path = context["templates_dict"]["received"]
        df = pd.read_csv(filepath_or_buffer=path)
        
        # convert followers column from string to integer
        df["followers"] = df["followers"].astype("int")
        # convert popularity column from string to integer
        df["popularity"] = df["popularity"].astype("int")
        # drop duplicates
        df = df.drop_duplicates()
        # drop NaN
        df = df.dropna()
        
        try: os.mkdir("/tmp/transformed")
        except FileExistsError: print("File exists: /tmp/transformed")
        
        tmp_file_path = "/tmp/transformed/file-{}-{}.csv".format(
            datetime.today().strftime("%Y%m%d%H%M%S"),
            "-".join(["spotify", "artist", "api"])
        )
        df.to_csv(tmp_file_path, index=False)
        context["ti"].xcom_push(key="tmp_file_path2", value=tmp_file_path)
        
        return {
            "Record number": df.shape[0],
            "column number": df.shape[1],
            "File path": tmp_file_path
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
                personal_page STRING, 
                followers INT,
                genres STRING,
                id STRING,
                name STRING,
                popularity INT,
                type STRING
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
        
    
    extract_data_from_spotify_api() >> transform_data_from_spotify_api() >> put_data_to_stage_in_snowflake
    create_schema_snowflake >> create_table_snowflake >> load_data_into_snowflake_from_file
    create_table_snowflake >> create_stage_table >> put_data_to_stage_in_snowflake
    put_data_to_stage_in_snowflake >> load_data_into_snowflake_from_file
    create_file_format >> put_data_to_stage_in_snowflake
    
etl_pipeline_spotifyApi_to_snowflake()
        
        
        