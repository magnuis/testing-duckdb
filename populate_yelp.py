import json
import duckdb
import time
import argparse
import os
from datetime import datetime
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Initialize file paths
JSON_FILE_PATH = './data/yelp/yelp_academic_dataset_user.json'

RAW_DB_PATH = 'raw_yelp.db'
MATERIALIZED_DB_PATH = 'materialized_yelp.db'
RESULTS_DB_PATH = 'test_results.db'

RAW_PARQUET_FILE_PATH = './data/yelp_raw_json.parquet'
MATERIALIZED_PARQUET_FILE_PATH = './data/yelp_materialized_json.parquet'


# Parse command-line arguments for metadata logging
def add_cmd_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Track JSON data insert times.")
    parser.add_argument('--environment', type=str, required=True,
                        help="The environment in which the test is run (e.g., 'VM', 'Local').")
    parser.add_argument('--tester', type=str, required=True,
                        help="Name of the person running the test.")
    return parser.parse_args()


def create_results_db(con: duckdb.DuckDBPyConnection):
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
    # Drop the users table if it exists, to ensure a fresh start
    con.execute("DROP TABLE IF EXISTS users")

    # Create tje users table with one data column
    con.execute("CREATE TABLE users (data JSON)")
    print("Created fresh users table for raw JSON data.")


def create_materialized_data_db(con: duckdb.DuckDBPyConnection):
    # Drop the users table if it exists, to ensure a fresh start
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
    print("Created fresh users table materialized JSON data.")


def parse_raw_json_to_parquet(batch_size=50000) -> int:
    print("Starting to parse raw JSON file and prepare for Parquet conversion...")

    # Initialize counters and batch storage
    data_batch = []
    total_rows = 0

    # Open a ParquetWriter for the first batch and use it for subsequent batches
    writer = None

    try:
        with open(JSON_FILE_PATH, 'r') as file:
            for line_number, line in enumerate(file, start=1):
                try:
                    # Parse the JSON document and store as a single string
                    json_obj = json.loads(line)
                    data_batch.append({'data': json.dumps(json_obj)})

                    # Once the batch reaches batch_size, write to Parquet
                    if len(data_batch) >= batch_size:
                        df_batch = pd.DataFrame(data_batch)
                        table_batch = pa.Table.from_pandas(df_batch)

                        # Initialize writer if it's the first batch
                        if writer is None:
                            writer = pq.ParquetWriter(
                                RAW_PARQUET_FILE_PATH, table_batch.schema)

                        # Write the current batch to Parquet
                        writer.write_table(table_batch)
                        total_rows += len(data_batch)
                        data_batch = []  # Clear batch memory
                        print(
                            f"Written {total_rows} rows to raw Parquet so far...")

                except json.JSONDecodeError as e:
                    print(f"Error decoding JSON on line {line_number}: {e}")

        # Write any remaining rows in the last batch
        if data_batch:
            df_batch = pd.DataFrame(data_batch)
            table_batch = pa.Table.from_pandas(df_batch)
            if writer is None:
                writer = pq.ParquetWriter(
                    RAW_PARQUET_FILE_PATH, table_batch.schema)
            writer.write_table(table_batch)
            total_rows += len(data_batch)
            print(f"""Final batch written. Total rows written to raw Parquet: {
                  total_rows}""")

    finally:
        # Close the writer if it was initialized
        if writer:
            writer.close()

    return total_rows


def parse_materialized_json_to_parquet(batch_size=50000) -> int:
    print("Starting to parse materialized JSON file and prepare for Parquet conversion...")

    data_batch = []
    total_rows = 0
    writer = None

    try:
        with open(JSON_FILE_PATH, 'r') as file:
            for line_number, line in enumerate(file, start=1):
                try:
                    # Parse the JSON document
                    json_obj = json.loads(line)
                    # Extract relevant fields, along with the entire JSON document as a string
                    data_batch.append({
                        'user_id': json_obj.get('user_id'),
                        'business_id': json_obj.get('business_id'),
                        'review_id': json_obj.get('review_id'),
                        'name': json_obj.get('name'),
                        'average_stars': json_obj.get('average_stars'),
                        'city': json_obj.get('city'),
                        'date': json_obj.get('date'),
                        'stars': json_obj.get('stars'),
                        'data': json.dumps(json_obj)
                    })

                    # Once the batch reaches the specified batch size, write to Parquet
                    if len(data_batch) >= batch_size:
                        df_batch = pd.DataFrame(data_batch)
                        table_batch = pa.Table.from_pandas(df_batch)

                        # Initialize ParquetWriter if it's the first batch
                        if writer is None:
                            writer = pq.ParquetWriter(
                                MATERIALIZED_PARQUET_FILE_PATH, table_batch.schema)

                        # Write the current batch to Parquet
                        writer.write_table(table_batch)
                        total_rows += len(data_batch)
                        data_batch = []  # Clear batch memory
                        print(
                            f"Written {total_rows} rows to Parquet so far...")

                except json.JSONDecodeError as e:
                    print(f"Error decoding JSON on line {line_number}: {e}")

        # Write any remaining rows in the last batch
        if data_batch:
            df_batch = pd.DataFrame(data_batch)
            table_batch = pa.Table.from_pandas(df_batch)
            if writer is None:
                writer = pq.ParquetWriter(
                    MATERIALIZED_PARQUET_FILE_PATH, table_batch.schema)
            writer.write_table(table_batch)
            total_rows += len(data_batch)
            print(f"Final batch written. Total rows written to materialized Parquet: {
                  total_rows}")

    finally:
        # Close the writer if it was initialized
        if writer:
            writer.close()

    print(f"""Parquet file with materialized JSON saved to {
          MATERIALIZED_PARQUET_FILE_PATH}""")
    return total_rows


def insert_parquet_into_db(con: duckdb.DuckDBPyConnection, no_lines: int, file_path=str) -> float:

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
