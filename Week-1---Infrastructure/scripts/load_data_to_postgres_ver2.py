import os
import argparse
import logging
import psycopg2
import psycopg2.extras
import pandas as pd

from time import time
from psycopg2 import sql



def load_data_to_postgres(params):
    
    # Connect to PostgreSQL database
    local = params.local
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    csv_name = params.csv_file
    
    if local == True:
        os.system(f"wget {csv_name} -O {csv_name}")
        
    db_conn = psycopg2.connect(
        host=host,
        port=port,
        database=db,
        user=user,
        password=password
    )
    
    # Load data from Parquet file in chunks
    df_iter: pd.DataFrame = pd.read_csv(
        csv_name, 
        iterator=True, 
        chunksize=100000, 
        index_col=False
    )
    
    with db_conn.cursor() as cursor:
        try:
            columns = sql.SQL(",").join(
                sql.Identifier(name.lower()) for name in df_iter.shape[1]
            )
            logging.info(f"Table {table_name} with columns: {columns}")
            values = sql.SQL(",").join(sql.Placeholder() for _ in df_iter.shape[1])
            
            logging.debug("Inserting data into temp table")
            insert_query = sql.SQL('INSERT INTO {} ({}) VALUES({});').format(
                sql.Identifier(table_name), columns, values
            )
            psycopg2.extras.execute_batch(cursor, insert_query, df_iter.shape[0])
            logging.info(f"Insert into data for table {table_name} Success !!!")
            
            db_conn.commit()
            
        except Exception as e:
            raise e
        
    
if __name__ == '__main__':
    
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--local', required=False, help='Check if running locally', default='False')
    parser.add_argument('--user', required=True, help='user name for postgres')
    parser.add_argument('--password', required=True, help='password for postgres')
    parser.add_argument('--host', required=True, help='host for postgres')
    parser.add_argument('--port', required=True, help='port for postgres')
    parser.add_argument('--db', required=True, help='database name for postgres')
    parser.add_argument('--table_name', required=True, help='name of the table where we will write the results to')
    parser.add_argument('--csv_file', required=True, help='csv_file of the csv file')

    args = parser.parse_args()              

    load_data_to_postgres(args)