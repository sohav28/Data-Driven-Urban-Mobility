import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

CLEAN_DATA_PATH = "data/processed/clean_trips_data"
TRIP_DURATION_COL = "trip_duration_seconds"  # Corrected
ROUTE_ID_COL = "ratecodeid"                 # Assumed/Corrected
PARTITION_COLUMN = "pickup_date"            # Corrected

def initialize_spark_session(app_name="TransitDataAnalysis"):
    """Initializes and returns a SparkSession."""
    # Ensure HADOOP_HOME is set for Windows compatibility during file access
    if os.name == 'nt':
        HADOOP_HOME_DIR = "C:\\hadoop"
        os.environ["HADOOP_HOME"] = HADOOP_HOME_DIR
        os.environ["PATH"] = os.environ.get("PATH", "") + ";" + HADOOP_HOME_DIR + "\\bin"
        
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    return spark

def run_pyspark_analysis(spark, input_path, trip_duration_col, route_id_col, partition_col):
    """
    Loads clean data and performs two key analytical tasks.
    """
    print("Loading clean, partitioned data...")
    # Read the partitioned Parquet data
    df = spark.read.parquet(input_path)
    
    # Register the DataFrame as a temporary SQL view
    df.createOrReplaceTempView("clean_trips")
    print(f"Loaded {df.count()} clean records.")

    # --- Analysis Task 1: Trip Count by Partition Column (e.g., Month) ---
    print("\n--- Analysis 1: Total Trip Count by Partition Column ---")
    query_1 = f"""
        SELECT
            {partition_col},
            COUNT(*) AS total_trips
        FROM clean_trips
        GROUP BY {partition_col}
        ORDER BY {partition_col}
    """
    trips_by_partition = spark.sql(query_1)
    
    print("Results:")
    trips_by_partition.show(10)

    # --- Analysis Task 2: Average Trip Duration for Top 5 Routes ---
    print("\n--- Analysis 2: Average Duration for Top 5 Routes (via DataFrame API) ---")
    
    # 1. Calculate trip count for all routes
    route_counts = df.groupBy(route_id_col).count().alias("route_counts")
    
    # 2. Get the top 5 routes
    top_5_routes = route_counts.orderBy(F.col("count").desc()).limit(5)
    
    # 3. Join back to the main DataFrame to filter only the top routes
    top_5_df = df.join(top_5_routes, route_id_col)
    
    # 4. Calculate average trip duration for these routes
    avg_duration_query = top_5_df.groupBy(route_id_col).agg(
        F.count("*").alias("Total_Trips"),
        F.mean(trip_duration_col).alias("Average_Trip_Duration")
    ).orderBy(F.col("Total_Trips").desc())
    
    print("Results:")
    avg_duration_query.show(5)

    print("\nPySpark Analysis complete. Results printed above.")

if __name__ == "__main__":
    spark_session = initialize_spark_session()
    
    try:
        run_pyspark_analysis(
            spark_session, 
            CLEAN_DATA_PATH, 
            TRIP_DURATION_COL, 
            ROUTE_ID_COL, 
            PARTITION_COLUMN
        )
    finally:
        if spark_session:
            spark_session.stop()