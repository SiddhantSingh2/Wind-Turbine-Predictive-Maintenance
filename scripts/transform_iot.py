from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, last, first, current_timestamp
from pyspark.sql.window import Window
import os

def main():
    spark = SparkSession.builder \
        .appName("WindTurbineSilverLayer") \
        .getOrCreate()

    # Paths (adjust based on your environment)
    bronze_path = "data/bronze/scada_iot/*.json"
    silver_path = "data/silver/turbine_telemetry"

    # 1. Read Bronze Data
    df_bronze = spark.read.json(bronze_path)

    # 2. Data Cleaning: Handle missing values using interpolation (Last Observation Carried Forward)
    # Define window for interpolation
    window_spec = Window.partitionBy("turbine_id").orderBy("timestamp").rowsBetween(Window.unboundedPreceding, 0)
    
    df_cleaned = df_bronze \
        .withColumn("wind_speed", last("wind_speed", ignorenulls=True).over(window_spec)) \
        .withColumn("power_output", last("power_output", ignorenulls=True).over(window_spec))

    # 3. Compute Silver Layer: 10-minute rolling average
    # Define window for rolling averages (assuming timestamp is in seconds, 10 mins = 600s)
    # Using rowsBetween if sampling is fixed, or rangeBetween for time-based windows
    rolling_window = Window.partitionBy("turbine_id").orderBy(col("timestamp").cast("long")).rangeBetween(-600, 0)

    df_silver = df_cleaned \
        .withColumn("avg_gearbox_temp_10m", avg("gearbox_temperature").over(rolling_window)) \
        .withColumn("avg_vibration_index_10m", avg("vibration_index").over(rolling_window)) \
        .withColumn("processed_at", current_timestamp())

    # 4. Save as Delta Table
    df_silver.write.format("delta").mode("overwrite").save(silver_path)
    print(f"Silver layer saved to {silver_path}")

if __name__ == "__main__":
    main()
