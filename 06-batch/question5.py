from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ===============================
# 1. Create Spark Session
# ===============================
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("question5") \
    .getOrCreate()

print("Spark UI:", spark.sparkContext.uiWebUrl)

# ===============================
# 2. Load Data
# ===============================

# Yellow November 2025
yellow = spark.read.parquet("yellow_tripdata_2025-11.parquet")

# Zone lookup CSV
zones = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv("taxi_zone_lookup.csv")

# ===============================
# 3. Create Temp Views
# ===============================
yellow.createOrReplaceTempView("yellow")
zones.createOrReplaceTempView("zones")

# ===============================
# 4. Find Least Frequent Pickup Zone
# ===============================
result = spark.sql("""
SELECT
    z.Zone,
    COUNT(*) AS trip_count
FROM yellow y
JOIN zones z
    ON y.PULocationID = z.LocationID
GROUP BY z.Zone
ORDER BY trip_count ASC
LIMIT 1
""")

result.show()

# ===============================
# 5. Stop Spark
# ===============================
spark.stop()