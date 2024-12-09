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
JSON_FILE_PATHS = ['./data/yelp/yelp_academic_dataset_business.json', './data/yelp/yelp_academic_dataset_checkin.json', './data/yelp/yelp_academic_dataset_review.json', './data/yelp/yelp_academic_dataset_user.json']
# JSON_FILE_PATHS = ['./data/yelp/yelp_academic_dataset_business.json', './data/yelp/yelp_academic_dataset_checkin.json', './data/yelp/yelp_academic_dataset_review.json', './data/yelp/yelp_academic_dataset_tip.json', './data/yelp/yelp_academic_dataset_user.json']

RAW_DB_PATH = 'raw_yelp.db'
MATERIALIZED_DB_PATH = 'materialized_yelp.db'
RESULTS_DB_PATH = 'test_results.db'

RAW_PARQUET_FILE_PATH = './data/yelp_raw_json.parquet'
MATERIALIZED_PARQUET_FILE_PATH = './data/yelp_materialized_json.parquet'


def create_raw_data_db(con: duckdb.DuckDBPyConnection):
    # Drop the users table if it exists, to ensure a fresh start
    con.execute("DROP TABLE IF EXISTS users")

    # Create tje users table with one data column
    con.execute("CREATE TABLE users (data JSON)")
    print("Created fresh users table for raw JSON data.")


def create_materialized_data_db(con: duckdb.DuckDBPyConnection):
    # Drop the users table if it exists, to ensure a fresh start
    con.execute("DROP TABLE IF EXISTS users")

    create_users_table_query = '''
    CREATE TABLE users (
        user_id VARCHAR,
        business_id VARCHAR,
        review_id VARCHAR,
        name VARCHAR,
        average_stars FLOAT,
        city VARCHAR,
        date VARCHAR,
        stars FLOAT,
        data JSON
    );
    '''
    con.execute(create_users_table_query)
    print("Created fresh users table for materialized JSON data.")


def parse_raw_json_to_parquet(file_path, con: duckdb.DuckDBPyConnection, batch_size=50000) -> int:
    print("Starting to parse raw JSON file and prepare for Parquet conversion...")

    writer = None
    data_batch = []
    total_rows = 0

    try:
        # for file_path in JSON_FILE_PATHS:
        with open(file_path, 'r') as file:
            for line_number, line in enumerate(file, start=1):
                try:
                    # Parse the JSON document and store as a single string
                    json_obj = json.loads(line)
                    data_batch.append({'data': json.dumps(json_obj)})

                    # Once the batch reaches batch_size, write to Parquet
                    if len(data_batch) >= batch_size:
                        df_batch = pd.DataFrame(data_batch)
                        table_batch = pa.Table.from_pandas(df_batch)
                        with pq.ParquetWriter(RAW_PARQUET_FILE_PATH, table_batch.schema) as writer:
                            writer.write_table(table_batch)
                        insert_parquet_into_db(con=con, file_path=RAW_PARQUET_FILE_PATH)
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
                with pq.ParquetWriter(RAW_PARQUET_FILE_PATH, table_batch.schema) as writer:
                    writer.write_table(table_batch)
                insert_parquet_into_db(con=con, file_path=RAW_PARQUET_FILE_PATH)
                total_rows += len(data_batch)
                print(f"""Final batch written. Total rows written to raw Parquet: {
                    total_rows}""")

    finally:
        # Close the writer if it was initialized
        if writer:
            writer.close()

    return total_rows


def parse_materialized_json_to_parquet(file_path, con: duckdb.DuckDBPyConnection, batch_size=50000) -> int:
    print("Starting to parse materialized JSON file and prepare for Parquet conversion...")

    data_batch = []
    total_rows = 0
    writer = None

    schema = pa.schema([
        ('user_id', pa.string(),True), 
        ('business_id', pa.string(),True), 
        ('review_id', pa.string(),True), 
        ('name', pa.string(),True), 
        ('average_stars', pa.float64(),True), 
        ('city', pa.string(),True), 
        ('date', pa.string(),True), 
        ('stars', pa.float64(),True),
        ('data', pa.string(), True) 
    ])
    try:
        # for file_path in JSON_FILE_PATHS:
        with open(file_path, 'r') as file:
            print(file_path)
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

                        df_batch['user_id'] = df_batch['user_id'].replace({None: pd.NA}).astype("string")
                        df_batch['business_id'] = df_batch['business_id'].replace({None: pd.NA}).astype("string")
                        df_batch['review_id'] = df_batch['review_id'].replace({None: pd.NA}).astype("string")
                        df_batch['name'] = df_batch['name'].replace({None: pd.NA}).astype("string")
                        df_batch['average_stars'] = pd.to_numeric(df_batch['average_stars'], errors='coerce')
                        df_batch['city'] = df_batch['city'].replace({None: pd.NA}).astype("string")
                        df_batch['date'] = df_batch['date'].replace({None: pd.NA}).astype("string")
                        df_batch['stars'] = pd.to_numeric(df_batch['stars'], errors='coerce')
                        df_batch['data'] = df_batch['data'].replace({None: pd.NA}).astype("string")

                        table_batch = pa.Table.from_pandas(df_batch)
                        with pq.ParquetWriter(MATERIALIZED_PARQUET_FILE_PATH, schema) as writer:
                            writer.write_table(table_batch)
                        insert_parquet_into_db(con=con, file_path=MATERIALIZED_PARQUET_FILE_PATH)
                        total_rows += len(data_batch)
                        data_batch = []  # Clear batch memory
                        print(
                            f"Written {total_rows} rows to materialized Parquet so far...")

                except json.JSONDecodeError as e:
                    print(f"Error decoding JSON on line {line_number}: {e}")

            # Write any remaining rows in the last batch
            if data_batch:
                df_batch = pd.DataFrame(data_batch)

                df_batch['user_id'] = df_batch['user_id'].replace({None: pd.NA}).astype("string")
                df_batch['business_id'] = df_batch['business_id'].replace({None: pd.NA}).astype("string")
                df_batch['review_id'] = df_batch['review_id'].replace({None: pd.NA}).astype("string")
                df_batch['name'] = df_batch['name'].replace({None: pd.NA}).astype("string")
                df_batch['average_stars'] = pd.to_numeric(df_batch['average_stars'], errors='coerce')
                df_batch['city'] = df_batch['city'].replace({None: pd.NA}).astype("string")
                df_batch['date'] = df_batch['date'].replace({None: pd.NA}).astype("string")
                df_batch['stars'] = pd.to_numeric(df_batch['stars'], errors='coerce')
                df_batch['data'] = df_batch['data'].replace({None: pd.NA}).astype("string")

                table_batch = pa.Table.from_pandas(df_batch)
                with pq.ParquetWriter(MATERIALIZED_PARQUET_FILE_PATH, schema) as writer:
                    writer.write_table(table_batch)
                insert_parquet_into_db(con=con, file_path=MATERIALIZED_PARQUET_FILE_PATH)
                total_rows += len(data_batch)
                print(f"""Final batch written. Total rows written to materialized Parquet: {
                    total_rows}""")

    finally:
        # Close the writer if it was initialized
        if writer:
            writer.close()

    print(f"""Parquet file with materialized JSON saved to {
          MATERIALIZED_PARQUET_FILE_PATH}""")
    return total_rows


def insert_parquet_into_db(con: duckdb.DuckDBPyConnection, file_path=str) -> float:

    print("Starting to insert raw JSON Parquet data into DuckDB...")
    start_time = time.perf_counter()  # Start timing

    # Insert Parquet data into DuckDB
    con.execute("BEGIN TRANSACTION")
    con.execute(
        f"INSERT INTO users SELECT * FROM read_parquet('{file_path}')")
    con.execute("COMMIT")

    end_time = time.perf_counter()  # End timing
    total_time = end_time - start_time

    # print(f"Inserted {no_lines} rows of raw JSON data into DuckDB.")
    return total_time




def clean_up():
    os.remove(RAW_PARQUET_FILE_PATH)
    print(f"Deleted file {RAW_PARQUET_FILE_PATH}")
    os.remove(MATERIALIZED_PARQUET_FILE_PATH)
    print(f"Deleted file {MATERIALIZED_PARQUET_FILE_PATH}")




# Connect to or create the DuckDB instances
raw_connection = duckdb.connect(RAW_DB_PATH)
materialized_connection = duckdb.connect(MATERIALIZED_DB_PATH)
# results_connection = duckdb.connect(RESULTS_DB_PATH)

# Clear and create required tables
create_raw_data_db(con=raw_connection)
create_materialized_data_db(con=materialized_connection)

for file in JSON_FILE_PATHS:

    # Parse the json into parquet
    raw_lines = parse_raw_json_to_parquet(con=raw_connection, file_path=file)
    materialized_lines = parse_materialized_json_to_parquet(con=materialized_connection, file_path=file)


# Delete created, unnecessary files
clean_up()
