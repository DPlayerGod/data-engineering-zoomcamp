FROM python:3.11-slim

RUN pip install pandas pyarrow sqlalchemy psycopg2-binary

WORKDIR /app

COPY load_data.py .
COPY green_tripdata_2025-11.parquet .
COPY taxi_zone_lookup.csv .

CMD ["python", "load_data.py"]
