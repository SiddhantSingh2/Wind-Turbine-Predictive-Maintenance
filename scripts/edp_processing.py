from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit
import os

def main():
    spark = SparkSession.builder \
        .appName("EDPDataProcessing") \
        .getOrCreate()

    # Paths
    raw_path = "data/raw_scada_2017.csv"
    processed_path = "data/silver/turbine_telemetry_edp"

    # 1. Read EDP Raw Data
    df_raw = spark.read.option("header", "true").option("inferSchema", "true").csv(raw_path)

    # 2. Map Columns and Handle Nulls
    # Mapping:
    # 'Wind Speed (m/s)' -> 'wind_speed'
    # 'Active Power (kW)' -> 'power_output'
    # 'Generator Bearing Temp (C)' -> 'bearing_temp'
    
    df_processed = df_raw.select(
        col("Wind Speed (m/s)").alias("wind_speed"),
        col("Active Power (kW)").alias("power_output"),
        col("Generator Bearing Temp (C)").alias("bearing_temp"),
        # Assuming there's a timestamp and turbine_id in the EDP dataset
        # If not, we'd typically add them or use what's available
        col("Timestamp").alias("timestamp") if "Timestamp" in df_raw.columns else lit(None).alias("timestamp"),
        col("Turbine_ID").alias("turbine_id") if "Turbine_ID" in df_raw.columns else lit("EDP_001").alias("turbine_id")
    )

    # Logic: Handle nulls in 'power_output' by filling with 0 if 'wind_speed' < 3m/s (cut-in speed)
    df_final = df_processed.withColumn(
        "power_output",
        when(col("power_output").isNull() & (col("wind_speed") < 3), 0)
        .otherwise(col("power_output"))
    )

    # 3. Save Processed Data
    df_final.write.mode("overwrite").parquet(processed_path)
    print(f"Processed EDP data saved to {processed_path}")

if __name__ == "__main__":
    main()
