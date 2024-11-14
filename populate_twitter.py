import os
import duckdb
import json
import time
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime

TOP_DIR = './data/twitter/06'
RAW_DB_PATH = 'raw_twitter.db'
MATERIALIZED_DB_PATH = 'materialized_twitter.db'
RAW_PARQUET_FILE_PATH = './data/twitter/twitter_raw_json.parquet'
MATERIALIZED_PARQUET_FILE_PATH = './data/twitter_materialized_json.parquet'

# Count the number of subdirectories in topdir
subdir_count = sum([1 for entry in os.scandir(TOP_DIR) if entry.is_dir()])

finished = 0


def create_raw_data_db(con: duckdb.DuckDBPyConnection):
    # Drop the tweets table if it exists, to ensure a fresh start
    con.execute("DROP TABLE IF EXISTS tweets")

    # Create tje tweets table with one data column
    con.execute("CREATE TABLE tweets (data JSON)")
    print("Created fresh tweets table for raw JSON data.")


def create_materialized_data_db(con: duckdb.DuckDBPyConnection):
    # Drop the twitter table if it exists, to ensure a fresh start
    con.execute("DROP TABLE IF EXISTS tweets")

    # Create the tweets table with individual columns for the materialized fields
    create_twitter_table_query = '''
    CREATE TABLE tweets (
        id_str VARCHAR,
        created_at TIMESTAMP,
        text VARCHAR,
        source VARCHAR,
        in_reply_to_status_id_str VARCHAR,
        in_reply_to_user_id_str VARCHAR,
        in_reply_to_screen_name VARCHAR,
        user_id_str VARCHAR,
        user_name VARCHAR,
        user_screen_name VARCHAR,
        user_followers_count INTEGER,
        user_verified BOOLEAN,
        lang VARCHAR,
        retweeted_status_id_str VARCHAR,
        hashtag_text VARCHAR
    );
    '''
    con.execute(create_twitter_table_query)
    print("Created fresh tweets table for materialized JSON data.")


def insert_parquet_into_db(con: duckdb.DuckDBPyConnection, file_path=str):

    print("Starting to insert raw JSON Parquet data into DuckDB...")
    start_time = time.perf_counter()  # Start timing

    # Insert Parquet data into DuckDB
    con.execute("BEGIN TRANSACTION")
    con.execute(
        f"""INSERT INTO tweets SELECT * FROM read_parquet('{file_path}')""")
    con.execute('COMMIT')

    end_time = time.perf_counter()  # End timing
    total_time = end_time - start_time

    print(f"""Inserted JSON data into DuckDB in {
          total_time} seconds.""")


def write_raw_json_to_parquet(file_path, batch_size=50000) -> int:

    writer = None
    data_batch = []
    total_rows = 0

    try:
        with open(file_path, 'r') as json_file:
            for line_number, line in enumerate(json_file, start=1):
                try:
                    # Parse the JSON document and store as a single string
                    json_obj = json.loads(line)
                    data_batch.append({'data': json.dumps(json_obj)})
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

    finally:
        # Close the writer if it was initialized
        if writer:
            writer.close()

    return total_rows


def write_materialized_json_to_parquet(file_path, batch_size=50000) -> int:
    writer = None
    data_batch = []
    total_rows = 0

    try:
        with open(file_path, 'r') as json_file:
            for line_number, line in enumerate(json_file, start=1):
                try:
                    # Parse the JSON document
                    json_obj = json.loads(line)


                    # Materialize JSON fields into separate columns
                    materialized_row = {
                        'id_str': json_obj.get('id_str'),
                        'created_at': datetime.strptime(json_obj.get('created_at'), '%a %b %d %H:%M:%S %z %Y'),
                        'text': json_obj.get('text'),
                        'source': json_obj.get('source'),
                        'in_reply_to_status_id_str': json_obj.get('in_reply_to_status_id_str'),
                        'in_reply_to_user_id_str': json_obj.get('in_reply_to_user_id_str'),
                        'in_reply_to_screen_name': json_obj.get('in_reply_to_screen_name'),
                        'user_id_str': json_obj.get('user', {}).get('id_str'),
                        'user_name': json_obj.get('user', {}).get('name'),
                        'user_screen_name': json_obj.get('user', {}).get('screen_name'),
                        'user_followers_count': json_obj.get('user', {}).get('followers_count'),
                        'user_verified': json_obj.get('user', {}).get('verified'),
                        'lang': json_obj.get('lang'),
                        'retweeted_status_id_str': json_obj.get('retweeted_status', {}).get('id_str'),
                        # Handle hashtags as a single string of comma-separated values
                        'hashtag_text': ','.join([h.get('text', '') for h in json_obj.get('entities', {}).get('hashtags', [])]),
                    }

                    data_batch.append(materialized_row)

                    if len(data_batch) >= batch_size:
                        df_batch = pd.DataFrame(data_batch)
                        table_batch = pa.Table.from_pandas(df_batch)

                        # Initialize writer if it's the first batch
                        if writer is None:
                            writer = pq.ParquetWriter(
                                MATERIALIZED_PARQUET_FILE_PATH, table_batch.schema)

                        # Write the current batch to Parquet
                        writer.write_table(table_batch)
                        total_rows += len(data_batch)
                        data_batch = []  # Clear batch memory

                except json.JSONDecodeError as e:
                    print(f"Error decoding JSON on line {line_number}: {e}")
                except TypeError as e:
                    continue

        # Write any remaining rows in the last batch
        if data_batch:
            df_batch = pd.DataFrame(data_batch)
            table_batch = pa.Table.from_pandas(df_batch)
            if writer is None:
                writer = pq.ParquetWriter(
                    MATERIALIZED_PARQUET_FILE_PATH, table_batch.schema)
            writer.write_table(table_batch)
            total_rows += len(data_batch)
            print(f"""Final batch written. Total rows written to materialized Parquet: {
                  total_rows}""")

    finally:
        # Close the writer if it was initialized
        if writer:
            writer.close()

    return total_rows


total_files = sum(
    len([file for file in files if file.endswith('json')])
    for _, _, files in os.walk(TOP_DIR)
)


for dirpath, _, files in os.walk(TOP_DIR):
    for file_name in files:
        if file_name.endswith('json'):
            file_path = os.path.join(dirpath, file_name)
            write_raw_json_to_parquet(file_path)
            write_materialized_json_to_parquet(file_path)

        finished += 1
        print(f"Finished with {finished}/{total_files} files")


raw_connection = duckdb.connect(RAW_DB_PATH)
materialized_connection = duckdb.connect(MATERIALIZED_DB_PATH)

# Clear and create required tables
create_raw_data_db(con=raw_connection)
create_materialized_data_db(con=materialized_connection)

insert_parquet_into_db(
    con=raw_connection, file_path=RAW_PARQUET_FILE_PATH)

insert_parquet_into_db(
    con=materialized_connection, file_path=MATERIALIZED_PARQUET_FILE_PATH)
