from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, expr, to_timestamp, from_unixtime
import os

def main():
    spark = SparkSession.builder \
        .appName("EDPDataProcessing") \
        .getOrCreate()

    # Paths
    raw_path = "data/raw_scada_2017.csv"
    processed_path = "data/silver/turbine_telemetry_edp"

    # 1. Read Raw Data (Inland Wind Farm Dataset)
    # Expected columns: "Sequence No.", "V", "D", "air density", "I", "S_b", "y (% relative to rated power)"
    df_raw = spark.read.option("header", "true").option("inferSchema", "true").csv(raw_path)

    # 2. Map Columns and Handle Missing Data
    # 'V' -> 'wind_speed'
    # 'y (% relative to rated power)' -> 'power_output' (Assume 2000kW rated power for conversion)
    # Synthesize 'bearing_temp' and 'timestamp'
    
    # Base timestamp: 2017-01-01 00:00:00 (Unix timestamp: 1483228800)
    start_timestamp = 1483228800
    
    df_processed = df_raw.select(
        col("V").alias("wind_speed"),
        (col("y (% relative to rated power)") * 20).alias("power_output"), # Convert % to approx kW (2000kW rated)
        lit("T01").alias("turbine_id"),
        col("Sequence No.").cast("long").alias("seq_no")
    ).withColumn(
        "timestamp", 
        (lit(start_timestamp) + (col("seq_no") - 1) * 600).cast("timestamp") # 10 min intervals
    ).withColumn(
        "bearing_temp",
        60 + (col("power_output") / 50) # Synthesize temp based on load
    )

    # Logic: Handle nulls in 'power_output' by filling with 0 if 'wind_speed' < 3m/s (cut-in speed)
    df_final = df_processed.withColumn(
        "power_output",
        when(col("power_output").isNull() & (col("wind_speed") < 3), 0)
        .otherwise(col("power_output"))
    ).drop("seq_no")

    # 3. Save Processed Data
    df_final.write.mode("overwrite").parquet(processed_path)
    print(f"Processed EDP (Inland) data saved to {processed_path}")

if __name__ == "__main__":
    main()
