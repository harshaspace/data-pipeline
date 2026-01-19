import pandas as pd
from sqlalchemy import create_engine

year = 2025
month = 11

taxi_data_url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_{year}-{month}.parquet'
zones_url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv'

# Read taxi zones data
zones_df = pd.read_csv(zones_url)
engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')

# Ingest to DB
zones_df.head(0).to_sql(name='taxi_zones', con=engine, if_exists='replace')
zones_df.to_sql(name='taxi_zones', con=engine, if_exists='append')

# Read green taxi data
green_taxi_df = pd.read_parquet(taxi_data_url)

# Ingest to DB
green_taxi_df.head(0).to_sql(name='green_taxi_trips', con=engine, if_exists='replace')
green_taxi_df.to_sql(name='green_taxi_trips', con=engine, if_exists='append')