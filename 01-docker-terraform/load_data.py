import pandas as pd
from sqlalchemy import create_engine

engine = create_engine(
    "postgresql://postgres:postgres@db:5432/ny_taxi"
)

print("Loading parquet...")
df = pd.read_parquet("green_tripdata_2025-11.parquet")

df.to_sql(
    "green_tripdata",
    engine,
    if_exists="replace",
    index=False
)

print("Loading zones...")
zones = pd.read_csv("taxi_zone_lookup.csv")

zones.to_sql(
    "zones",
    engine,
    if_exists="replace",
    index=False
)

print("DONE")
