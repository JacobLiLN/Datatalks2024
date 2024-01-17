import argparse

import pandas as pd
from sqlalchemy import create_engine
from time import time

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    csv_name = 'output.csv'

    # download the csv

    engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}")

    print(pd.io.sql.get_schema(df, name=table_name, con=engine))

    # Read the whole csv file rather than only first 100 lines of the original data
    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)

    df = next(df_iter)
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')


    for df in df_iter:
        t_start = time()
    
        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
        df.to_sql(name=table_name, con=engine, if_exists='append')

        t_end = time()
        print('inserted another chunk, took {} seconds'.format(t_end-t_start))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    # user, password, host, port, database name, table name
    # url of the csv

    parser.add_argument('user',help='user name for postgres')
    parser.add_argument('password',help='user password for postgres')
    parser.add_argument('host',help='host for postgres')
    parser.add_argument('port',help='port number for postgres')
    parser.add_argument('database name',help='database name for postgres')
    parser.add_argument('table name',help='name of table where we will write the results to')
    parser.add_argument('url',help='url of the csv file')

    args = parser.parse_args()
    print(args.accumulate(args.integers))
    main(args)
