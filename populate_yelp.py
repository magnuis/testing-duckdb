import json
import duckdb
import time
import argparse
import os
from datetime import datetime
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Specify file paths
JSON_FILE_PATH = './data/yelp/yelp_academic_dataset_user.json'

RAW_DB_PATH = 'raw_yelp.db'
MATERIALIZED_DB_PATH = 'materialized_yelp.db'
RESULTS_DB_PATH = 'test_results.db'

RAW_PARQUET_FILE_PATH = './data/yelp_raw_json.parquet'
MATERIALIZED_PARQUET_FILE_PATH = './data/yelp_materialized_json.parquet'


# Parse command-line arguments for metadata
def add_cmd_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Track JSON data insert times.")
    parser.add_argument('--environment', type=str, required=True,
                        help="The environment in which the test is run (e.g., 'VM', 'Local').")
    parser.add_argument('--tester', type=str, required=True,
                        help="Name of the person running the test.")
    return parser.parse_args()


def create_results_db(con: duckdb.DuckDBPyConnection):
    # Create table for storing test results if it doesn't exist
    create_results_table_query = '''
    CREATE TABLE IF NOT EXISTS insert_test_results (
        run_id UUID DEFAULT uuid(),
        timestamp TIMESTAMP,
        environment VARCHAR,
        tester VARCHAR,
        total_inserts INTEGER,
        total_time_seconds FLOAT,
        db_size_bytes BIGINT,
        dataset VARCHAR,
        comment VARCHAR
    );
    '''
    con.execute(create_results_table_query)
    print("Created or confirmed existence of insert_test_results table.")


def create_raw_data_db(con: duckdb.DuckDBPyConnection):

    # Ensure the users table only has a single JSON column for raw inserts
    con.execute("DROP TABLE IF EXISTS users")
    con.execute("CREATE TABLE users (data JSON)")
    print("Created fresh users table for raw JSON data.")


def create_materialized_data_db(con: duckdb.DuckDBPyConnection):
    # Drop the users table if it exists to ensure a fresh start
    con.execute("DROP TABLE IF EXISTS users")

    # Create the users table with individual columns for the materialized fields
    create_users_table_query = '''
    CREATE TABLE users (
        user_id VARCHAR,
        business_id VARCHAR,
        review_id VARCHAR,
        name VARCHAR,
        average_stars FLOAT,
        city VARCHAR,
        date DATE,
        stars FLOAT,
        data VARCHAR
    );
    '''
    con.execute(create_users_table_query)
    print("Created fresh users table with materialized fields for efficient querying.")


def parse_raw_json_to_parquet() -> int:
    print("Starting to parse raw JSON file and prepare for Parquet conversion...")
    data_raw = []

    with open(JSON_FILE_PATH, 'r') as file:
        for line_number, line in enumerate(file, start=1):
            try:
                # Parse the JSON document and store as a single string
                json_obj = json.loads(line)
                data_raw.append({'data': json.dumps(json_obj)})

                # Print progress every 100,000 lines
                if line_number % 500000 == 0:
                    print(f"Processed {line_number} lines...")

            except json.JSONDecodeError as e:
                print(f"Error decoding JSON on line {line_number}: {e}")

    print("Finished parsing JSON file for raw Parquet conversion.")

    # Convert to Parquet and save
    df_raw = pd.DataFrame(data_raw)
    table_raw = pa.Table.from_pandas(df_raw)
    pq.write_table(table_raw, RAW_PARQUET_FILE_PATH)
    print(f"Parquet file with raw JSON saved to {RAW_PARQUET_FILE_PATH}")

    return len(df_raw)


def parse_materialized_json_to_parquet() -> int:
    print("Starting to parse materialized JSON file and prepare for Parquet conversion...")

    data_materialized = []
    with open(JSON_FILE_PATH, 'r') as file:
        for line_number, line in enumerate(file, start=1):
            try:
                # Parse the JSON document
                json_obj = json.loads(line)
                # Add relevant fields along with the entire JSON document as a string
                data_materialized.append({
                    'user_id': json_obj.get('user_id'),
                    'business_id': json_obj.get('business_id'),
                    'review_id': json_obj.get('review_id'),
                    'name': json_obj.get('name'),
                    'average_stars': json_obj.get('average_stars'),
                    'city': json_obj.get('city'),
                    'date': json_obj.get('date'),
                    'stars': json_obj.get('stars'),
                    # Store the entire JSON document as a string
                    'data': json.dumps(json_obj)
                })

                # Print progress every 100,000 lines for tracking
                if line_number % 500000 == 0:
                    print(f"Processed {line_number} lines...")

            except json.JSONDecodeError as e:
                print(f"Error decoding JSON on line {line_number}: {e}")

    print("Finished parsing JSON file for raw Parquet conversion.")

    # Convert to Parquet and save
    df_materialized = pd.DataFrame(data_materialized)
    table_materialized = pa.Table.from_pandas(df_materialized)
    pq.write_table(table_materialized, MATERIALIZED_PARQUET_FILE_PATH)
    print(f"""Parquet file with materialized JSON saved to {
          MATERIALIZED_PARQUET_FILE_PATH}""")

    return len(df_materialized)


def insert_parquet_into_db(con: duckdb.DuckDBPyConnection, no_lines: int, file_path=str) -> float:

    # Step 2: Insert Raw JSON Parquet File into DuckDB
    print("Starting to insert raw JSON Parquet data into DuckDB...")
    start_time = time.perf_counter()  # Start timing

    # Insert Parquet data into DuckDB
    con.execute("BEGIN TRANSACTION")
    con.execute(
        f"INSERT INTO users SELECT * FROM read_parquet('{file_path}')")
    con.execute("COMMIT")

    end_time = time.perf_counter()  # End timing
    total_time = end_time - start_time

    print(f"Inserted {no_lines} rows of raw JSON data into DuckDB.")
    return total_time


def log_results(con: duckdb.DuckDBPyConnection, env: str, tester: str, no_lines: int, insert_time: float, comment: str):
    db_size = os.path.getsize(RAW_DB_PATH)
    print(f"Database size: {db_size} bytes.")

    print("Logging results for raw JSON insert test...")

    con.execute('''
        INSERT INTO insert_test_results (timestamp, environment, tester,  total_inserts, total_time_seconds,db_size_bytes, dataset, comment)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ''', (datetime.now(), env, tester, no_lines, insert_time, db_size, "yelp", comment))

    print(f"""Raw JSON data inserted and logged successfully in {
        insert_time:.2f} seconds. """)


def clean_up():
    os.remove(RAW_PARQUET_FILE_PATH)
    print(f"Deleted file {RAW_PARQUET_FILE_PATH}")
    os.remove(MATERIALIZED_PARQUET_FILE_PATH)
    print(f"Deleted file {MATERIALIZED_PARQUET_FILE_PATH}")


args = add_cmd_args()


# Connect to or create the DuckDB instances
raw_connection = duckdb.connect(RAW_DB_PATH)
materialized_connection = duckdb.connect(MATERIALIZED_DB_PATH)
results_connection = duckdb.connect(RESULTS_DB_PATH)

# Clear and create required tables
create_raw_data_db(con=raw_connection)
create_materialized_data_db(con=materialized_connection)
create_results_db(con=results_connection)

# Parse the json into parquet
raw_lines = parse_raw_json_to_parquet()
materialized_lines = parse_materialized_json_to_parquet()

# Insert parquet into db
raw_insert_time = insert_parquet_into_db(
    con=raw_connection, no_lines=raw_lines, file_path=RAW_PARQUET_FILE_PATH)
materialized_insert_time = insert_parquet_into_db(
    con=materialized_connection, no_lines=materialized_lines, file_path=MATERIALIZED_PARQUET_FILE_PATH)

# Log the results
env = args.environment
tester = args.tester

log_results(con=results_connection, env=env, tester=tester,
            no_lines=raw_lines, insert_time=raw_insert_time, comment="raw JSON insert from Parquet")
log_results(con=results_connection, env=env, tester=tester,
            no_lines=materialized_lines, insert_time=materialized_insert_time, comment="materialized JSON insert from Parquet")

# Delete created, unnecessary files
clean_up()
