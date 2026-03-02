from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("question4") \
    .getOrCreate()


df = spark.read.parquet("yellow_tripdata_2025-11.parquet")

df2 = df \
    .withColumn("pickup_ts", F.to_timestamp("tpep_pickup_datetime")) \
    .withColumn("dropoff_ts", F.to_timestamp("tpep_dropoff_datetime")) \
    .withColumn(
        "trip_hours",
        (F.unix_seconds("dropoff_ts") - F.unix_seconds("pickup_ts")) / 3600.0
    )

max_hours = df2.select(F.max("trip_hours").alias("max_hours")).first()["max_hours"]
print("Longest trip (hours):", max_hours)

spark.stop()