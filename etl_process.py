import os
# Define the path where you placed the winutils.exe directory
HADOOP_HOME_DIR = "C:\\hadoop" 
# Set the environment variable
os.environ["HADOOP_HOME"] = HADOOP_HOME_DIR
os.environ["PATH"] = os.environ["PATH"] + ";" + HADOOP_HOME_DIR + "\\bin"

from pyspark.sql import SparkSession
# Added 'unix_timestamp' to the imports
from pyspark.sql.functions import col, lit, round, to_date, unix_timestamp 

# --- Configuration ---
FULL_RAW_DATA_PATH = 'data/processed/full_raw_data.parquet'
CLEAN_DATA_PATH = 'data/processed/clean_trips_data'
PARTITION_COLUMN = 'pickup_date' 

# --- 1. Initialize PySpark Session ---
def init_spark_session():
    """Initializes and returns a SparkSession."""
    spark = SparkSession.builder \
        .appName("TaxiETLProcess") \
        .config("spark.executor.memory", "2g") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    print("Spark Session Initialized.")
    return spark

# --- 2. PySpark ETL ---
def run_pyspark_etl(spark: SparkSession, input_path: str, output_path: str, partition_col: str):
    print(f"Reading raw data from {input_path}...")
    
    # E (Extract): Read the data saved by the Pandas script in Step 1
    taxi_df = spark.read.parquet(input_path)
    print(f"Initial row count: {taxi_df.count()}")

    # T (Transform): Apply transformations and feature engineering
    # Calculate Trip Duration (in seconds) using unix_timestamp for robustness
    duration_sec = (unix_timestamp(col("tpep_dropoff_datetime")) - unix_timestamp(col("tpep_pickup_datetime")))
    
    # Calculate Average Speed (miles per hour)
    # trip_distance / (duration_sec / 3600 seconds per hour)
    avg_speed = (col("trip_distance") / (duration_sec / lit(3600)))
    
    # Extract Pickup Date for Partitioning
    pickup_date = to_date(col("tpep_pickup_datetime")).alias(partition_col)
    
    transformed_df = taxi_df.withColumn("trip_duration_seconds", duration_sec) \
                        .withColumn("avg_speed_mph", round(avg_speed, 2)) \
                        .withColumn(partition_col, pickup_date)    
    # --- Data Cleaning (Part of T) ---
    # Filter out impossible/bad records 
    cleaned_df = transformed_df.filter(col("trip_duration_seconds") > 0) \
                        .filter(col("passenger_count") > 0) \
                        .filter(col("fare_amount") > 0) \
                        .filter(col("trip_distance") > 0)
    # Select final, cleaned columns
    final_cols = [
        "tpep_pickup_datetime", "tpep_dropoff_datetime", partition_col,
        "passenger_count", "trip_distance", "fare_amount", "tip_amount",
        "tolls_amount", "total_amount", "payment_type", 
        "trip_duration_seconds", "avg_speed_mph", "ratecodeid" 
    ]
    
    final_df = cleaned_df.select(*final_cols)
    
    print(f"Transformed and cleaned row count (after filtering): {final_df.count()}")

    # L (Load): Write the transformed, clean data to Parquet
    print(f"Writing partitioned data to {output_path}...")
    final_df.write \
        .mode("overwrite") \
        .partitionBy(partition_col) \
        .parquet(output_path)
    
    print("PySpark ETL complete. Clean data written to disk.")


# --- Execution ---
if __name__ == "__main__":
    spark = init_spark_session()
    run_pyspark_etl(spark, FULL_RAW_DATA_PATH, CLEAN_DATA_PATH, PARTITION_COLUMN)
    spark.stop()
    print("\n--- Step 2 Complete ---")
    print(f"Clean, partitioned data is located at: {CLEAN_DATA_PATH}")