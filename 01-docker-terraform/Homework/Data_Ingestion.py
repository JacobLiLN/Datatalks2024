from sqlalchemy import create_engine
import pandas as pd
import argparse
import os
from time import time

# User, password, host, port, database, tablename, csv1, csv2

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    taxi_table = params.taxi_table
    zone_table = params.zone_table
    url_taxi = params.url_taxi
    url_zone = params.url_zone

    if url_taxi.endswith('.csv.gz'):
        csv_taxi = 'taxi_output.csv.gz'
        os.system(f'wget {url_taxi} -O {csv_taxi}')
        os.system(f'gzip -d {csv_taxi}')
        csv_taxi = 'taxi_output.csv'
    else:
        csv_taxi = 'taxi_output.csv'
        os.system(f'wget {url_taxi} -O {csv_taxi}')
    
    if url_zone.endswith('.csv.gz'):
        csv_zone = 'zone_output.csv.gz'
        os.system(f'wget {url_zone} -O {csv_zone}')
        os.system(f'gzip -d {csv_zone}')
        csv_zone = 'zone_output.csv'
    else:
        csv_zone = 'zone_output.csv'
        os.system(f'wget {url_zone} -O {csv_zone}')

    
    engine =create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")

    df_iter = pd.read_csv(csv_taxi,iterator=True,chunksize=50000) #dtype=str,
    df = next(df_iter)

    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

    # Create Schema
    df.head(0).to_sql(name=taxi_table, con=engine, if_exists='replace')

    df.to_sql(name=taxi_table, con=engine, if_exists='append')

    while True:
        try:
            t_start = time()

            df=next(df_iter)

            df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
            df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

            df.to_sql(name=taxi_table, con=engine, if_exists='append')

            t_end = time()

            print('inserted another chunk, took %.3f second' % (t_end - t_start))

        except StopIteration:
            print("Finished ingesting data into the postgres database")
            break
    
    df_z = pd.read_csv(csv_zone)
    df_z.to_sql(name=zone_table, con=engine, if_exists='replace')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data with parameters')

    parser.add_argument('--user',required=True,help='user name for postgres')
    parser.add_argument('--password',required=True,help='user password for postgres')
    parser.add_argument('--host',required=True,help='host for postgres')
    parser.add_argument('--port',required=True,help='port number for postgres')
    parser.add_argument('--db',required=True,help='database name for postgres')
    parser.add_argument('--taxi_table',required=True,help='name of taxi table')
    parser.add_argument('--zone_table',required=True,help='name of zone table')
    parser.add_argument('--url_taxi',required=True,help='url of the taxi rides file')
    parser.add_argument('--url_zone',required=True,help='url of the zone file')

    args = parser.parse_args()

    main(args)
    







