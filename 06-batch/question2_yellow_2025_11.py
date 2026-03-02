import os
from pyspark.sql import SparkSession

# ===============================
# 1. Create Spark Session
# ===============================
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("yellow_2025_11") \
    .getOrCreate()

print(f"Spark version: {spark.version}")

# ===============================
# 2. Read November 2025 Yellow file
# ===============================
input_path = "yellow_tripdata_2025-11.parquet"  # <-- sửa nếu cần

df = spark.read.parquet(input_path)

print("Initial partitions:", df.rdd.getNumPartitions())

# ===============================
# 3. Repartition to 4 partitions
# ===============================
df_repartitioned = df.repartition(4)

print("Partitions after repartition:", df_repartitioned.rdd.getNumPartitions())

# ===============================
# 4. Write to parquet
# ===============================
output_path = "yellow_2025_11_repartitioned"

df_repartitioned.write.mode("overwrite").parquet(output_path)

print("Data written successfully.")

# ===============================
# 5. Calculate average parquet file size
# ===============================
files = [
    os.path.join(output_path, f)
    for f in os.listdir(output_path)
    if f.endswith(".parquet")
]

sizes = [os.path.getsize(f) for f in files]  # bytes

avg_size_mb = sum(sizes) / len(sizes) / (1024 * 1024)

print(f"Average Parquet file size: {avg_size_mb:.2f} MB")

# ===============================
# 6. Stop Spark
# ===============================
spark.stop()