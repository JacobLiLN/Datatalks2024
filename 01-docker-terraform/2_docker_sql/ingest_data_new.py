import io
import os
import requests
import pandas as pd

import argparse
from sqlalchemy import create_engine

# services = ['fhv','green','yellow']
init_url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/'
# switch out the bucketname


def compose_request_url(year, month, service):
    # csv file_name
    file_name = f"{service}_tripdata_{year}-{month}.csv.gz"

    # download it using requests via a pandas df
    request_url = f"{init_url}{service}/{file_name}"
    # r = requests.get(request_url)
    
    return request_url
        
def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    year = params.year
    month = params.month
    service = params.service

    month = '0'+str(int(month))
    month = month[-2:]

    csv_name = f"{service}_tripdata_{year}-{month}.csv.gz"
    url = f"{init_url}{service}/{csv_name}"
    

    os.system(f"wget {url} -O {csv_name}")
    print(url)

    engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}")

    df_iter = pd.read_csv(csv_name, compression='gzip' , iterator=True, chunksize=100000)

    df = next(df_iter)

    if service == 'yellow':
        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    elif service == 'green':
        df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
        df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

    for df in df_iter:
        # tpep for yellow, lpep for green

        if service == 'yellow':
            df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
            df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
        elif service == 'green':
            df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
            df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
        
        df.to_sql(name=table_name, con=engine, if_exists='append')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')
    parser.add_argument('--user',required=True,help='user name for postgres')
    parser.add_argument('--password',required=True,help='user password for postgres')
    parser.add_argument('--host',required=True,help='host for postgres')
    parser.add_argument('--port',required=True,help='port number for postgres')
    parser.add_argument('--db',required=True,help='database name for postgres')
    parser.add_argument('--table_name',required=True,help='name of table where we will write the results to')
    parser.add_argument('--year',required=True,help='year of the ny taxi data')
    parser.add_argument('--month',required=True,help='month of the ny taxi data')
    parser.add_argument('--service',required=True,help='service of the ny taxi data')

    args = parser.parse_args()
    main(args)