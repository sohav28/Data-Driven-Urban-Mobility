import requests
import pandas as pd
from sqlalchemy import create_engine
import os

# --- Configuration ---
# Using January 2024 Yellow Taxi data for a large dataset example (Parquet format)
DATA_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
RAW_FILE_PATH = 'data/raw/yellow_tripdata_2024-01.parquet'
SQLITE_DB_PATH = 'data/taxi_data.db'
SQL_TABLE_NAME = 'raw_trips'

# Create necessary directories
os.makedirs('data/raw', exist_ok=True)
os.makedirs('data/processed', exist_ok=True)

# --- 1. Download Data ---
def download_data(url: str, path: str):
    """Downloads the data file if it doesn't already exist."""
    if os.path.exists(path):
        print(f"File already exists at {path}. Skipping download.")
        return
    print(f"Downloading data from {url}...")
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status() # Check for bad status code
        with open(path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f"Download complete and saved to {path}.")
    except requests.exceptions.RequestException as e:
        print(f"Error during download: {e}")
        # Exit or handle error gracefully

# --- 2. Initial Load and Save to SQL (Optional for exploration) ---
def initial_load_and_setup(raw_path: str, db_path: str, table_name: str):
    """Loads a sample of the data into a Pandas DataFrame and loads it to SQLite."""
    print("Loading data into Pandas and saving a sample to SQLite...")
    
    # Read the full Parquet file
    df = pd.read_parquet(raw_path)
    print(f"Raw data shape: {df.shape}")

    # Clean up column names for easier SQL/Pandas access
    df.columns = df.columns.str.lower().str.replace(' ', '_')

    # Drop potential PII or complex columns not needed immediately
    df = df.drop(columns=['airport_fee', 'pulocationid', 'dolocationid'], errors='ignore')

    # Convert to appropriate types (initial cleaning)
    for col in ['tpep_pickup_datetime', 'tpep_dropoff_datetime']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')

    # Create a small sample for fast SQL exploration
    sample_df = df.head(100000)

    # Setup SQLAlchemy engine for SQLite
    engine = create_engine(f'sqlite:///{db_path}')

    # Save the sample to SQLite
    print(f"Loading {len(sample_df)} rows into SQLite table '{table_name}'...")
    sample_df.to_sql(table_name, engine, if_exists='replace', index=False)
    print(f"SQLite load complete. DB available at {db_path}")

    # Save the full, raw DataFrame to a new Parquet file in the 'processed' folder
    # This is important for the large-scale PySpark step later.
    full_processed_path = 'data/processed/full_raw_data.parquet'
    df.to_parquet(full_processed_path)
    print(f"Full raw data saved to: {full_processed_path}")
    return full_processed_path


# --- Execution ---
if __name__ == "__main__":
    download_data(DATA_URL, RAW_FILE_PATH)
    full_parquet_path = initial_load_and_setup(RAW_FILE_PATH, SQLITE_DB_PATH, SQL_TABLE_NAME)
    print("\n--- Step 1 Complete ---")
    print(f"The full data path for next steps is: {full_parquet_path}")