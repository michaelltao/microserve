from pyspark.sql import functions as F

def compute_driver_stats(df):
    driver_stats = df.groupby("VendorID").agg(
        F.sum("passenger_count").alias("total_passengers"),
        F.count('VendorID').alias('trip_count')
    )

    return driver_stats