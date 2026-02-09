from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from feature import Feature

import importlib.util

spark = SparkSession.builder \
    .appName("TaxiFeaturePipeline") \
    .master("local[*]") \
    .getOrCreate()

raw_df = spark.read.parquet("data/yellow_tripdata_2025-01.parquet", header=True, inferSchema=True)

driver_stats = Feature(
    "vendor stats",
    "transformations/compute_driver_stats.py"
)

feature_df = driver_stats.compute(raw_df)

output_path = "./feature_store/driver_stats"
feature_df.write.mode("overwrite").parquet(output_path)

print(f"Features successfully saved to {output_path}")