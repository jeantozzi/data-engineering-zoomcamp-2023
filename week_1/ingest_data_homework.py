#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import os
from sqlalchemy import create_engine
from time import time
import argparse

def main(params):
    # Unpacking the parameters from parser
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url

    # Downloading the CSV file
    csv_name = 'output.csv'
    os.system(f'wget -qO - {url} | gzip -d > {csv_name}')
    os.system('wget https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv -O zones.csv')

    # Creating engine: db://user:pass@host:port/table
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    # Creating a iterator that processes 100k rows per step
    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000, low_memory=False)

    # Creating tables based on the schema
    df_data = pd.read_csv(csv_name, nrows=1)
    df_data.lpep_pickup_datetime = pd.to_datetime(df_data.lpep_pickup_datetime)
    df_data.lpep_dropoff_datetime = pd.to_datetime(df_data.lpep_dropoff_datetime)
    df_data.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
    print(f'{table_name} table created...')

    df_zones = pd.read_csv('zones.csv')
    df_zones.head(n=0).to_sql(name='zones', con=engine, if_exists='replace')
    print('zones table created...')
    df_zones.to_sql(name='zones', con=engine, if_exists='append')
    print('zones table ingested...')

    # Inserting all data
    for index, chunk in enumerate(df_iter):
        t_start = time()
        
        # Changing date from Text to Datetime
        chunk.lpep_pickup_datetime = pd.to_datetime(chunk.lpep_pickup_datetime)
        chunk.lpep_dropoff_datetime = pd.to_datetime(chunk.lpep_dropoff_datetime)
        
        # Inserting data chunk from iterator
        chunk.to_sql(name=table_name, con=engine, if_exists='append')
        
        t_end = time()
        
        print(f'Inserting chunk #{index + 1}... Number of rows: {len(chunk)}, Time elapsed: {(t_end - t_start):.3f}s')

if __name__ == '__main__':
    # Creating parser for arguments
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')
    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='name of the table where we will write the results to')
    parser.add_argument('--url', help='url of the csv file')
    args = parser.parse_args()
    
    main(args)