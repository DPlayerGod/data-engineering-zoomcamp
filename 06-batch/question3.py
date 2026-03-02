from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# 1. Create Spark session
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("question3") \
    .getOrCreate()

# 2. Read parquet
df = spark.read.parquet("yellow_tripdata_2025-11.parquet")

# 3. Filter trips on 15 Nov 2025
cnt = (
    df.filter(
        F.to_date("tpep_pickup_datetime") == F.lit("2025-11-15")
    ).count()
)

print("Trips on 15 Nov 2025:", cnt)

spark.stop()