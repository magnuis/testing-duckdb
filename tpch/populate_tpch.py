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
JSON_FILE_PATH = './data/tpch_json.json'

RAW_DB_PATH = 'raw_tpch.db'
MATERIALIZED_DB_PATH = 'materialized_tpch.db'

RAW_PARQUET_FILE_PATH = './data/tpch_raw_json.parquet'
MATERIALIZED_PARQUET_FILE_PATH = './data/tpch_materialized_json.parquet'

# Table definitions with explicit types
table_column_types = {
    'C_CUSTKEY': 'BIGINT', 'C_NAME': 'VARCHAR', 'C_ADDRESS': 'VARCHAR', 
    'C_NATIONKEY': 'BIGINT', 'C_PHONE': 'VARCHAR', 'C_ACCTBAL': 'DOUBLE',
    'C_MKTSEGMENT': 'VARCHAR', 'C_COMMENT': 'VARCHAR',
    
    'L_ORDERKEY': 'BIGINT', 'L_PARTKEY': 'BIGINT', 'L_SUPPKEY': 'BIGINT',
    'L_LINENUMBER': 'BIGINT', 'L_QUANTITY': 'DOUBLE', 'L_EXTENDEDPRICE': 'DOUBLE',
    'L_DISCOUNT': 'DOUBLE', 'L_TAX': 'DOUBLE', 'L_RETURNFLAG': 'VARCHAR',
    'L_LINESTATUS': 'VARCHAR', 'L_SHIPDATE': 'DATE', 'L_COMMITDATE': 'DATE',
    'L_RECEIPTDATE': 'DATE', 'L_SHIPINSTRUCT': 'VARCHAR', 'L_SHIPMODE': 'VARCHAR', 'L_COMMENT': 'VARCHAR',
    
    'N_NATIONKEY': 'BIGINT', 'N_NAME': 'VARCHAR', 'N_REGIONKEY': 'BIGINT', 'N_COMMENT': 'VARCHAR',
    
    'O_ORDERKEY': 'BIGINT', 'O_CUSTKEY': 'BIGINT', 'O_ORDERSTATUS': 'VARCHAR', 
    'O_TOTALPRICE': 'DOUBLE', 'O_ORDERDATE': 'DATE', 'O_ORDERPRIORITY': 'VARCHAR',
    'O_CLERK': 'VARCHAR', 'O_SHIPPRIORITY': 'BIGINT', 'O_COMMENT': 'VARCHAR',
    
    'P_PARTKEY': 'BIGINT', 'P_NAME': 'VARCHAR', 'P_MFGR': 'VARCHAR', 'P_BRAND': 'VARCHAR',
    'P_TYPE': 'VARCHAR', 'P_SIZE': 'BIGINT', 'P_CONTAINER': 'VARCHAR',
    'P_RETAILPRICE': 'DOUBLE', 'P_COMMENT': 'VARCHAR',
    
    'PS_PARTKEY': 'BIGINT', 'PS_SUPPKEY': 'BIGINT', 'PS_AVAILQTY': 'BIGINT',
    'PS_SUPPLYCOST': 'DOUBLE', 'PS_COMMENT': 'VARCHAR',
    
    'R_REGIONKEY': 'BIGINT', 'R_NAME': 'VARCHAR', 'R_COMMENT': 'VARCHAR',
    
    'S_SUPPKEY': 'BIGINT', 'S_NAME': 'VARCHAR', 'S_ADDRESS': 'VARCHAR',
    'S_NATIONKEY': 'BIGINT', 'S_PHONE': 'VARCHAR', 'S_ACCTBAL': 'DOUBLE', 'S_COMMENT': 'VARCHAR',
    
    'data': 'VARCHAR'  # Added as the JSON string data column
}

# Parse command-line arguments for metadata logging
def add_cmd_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Track JSON data insert times.")
    parser.add_argument('--environment', type=str, required=True,
                        help="The environment in which the test is run (e.g., 'VM', 'Local').")
    parser.add_argument('--tester', type=str, required=True,
                        help="Name of the person running the test.")
    return parser.parse_args()



def create_raw_data_db(con: duckdb.DuckDBPyConnection):
    # Drop the tpch_data table if it exists, to ensure a fresh start
    con.execute("DROP TABLE IF EXISTS tpch_data")

    # Create the tpch_data table with one data column
    con.execute("CREATE TABLE tpch_data (data JSON)")
    print("Created fresh tpch_data table for raw JSON data.")


def create_materialized_data_db(con: duckdb.DuckDBPyConnection, all_columns, column_types):
    # Drop the tpch_data table if it exists, to ensure a fresh start
    con.execute("DROP TABLE IF EXISTS tpch_data")
    

    # Construct the CREATE TABLE query
    create_table_query = "CREATE TABLE tpch_data ("
    create_table_query += ", ".join([
        f"{col} {column_types[col]}" for col in all_columns
    ])
    create_table_query += ");"

    con.execute(create_table_query)
    print("Created fresh tpch_data table for materialized JSON data.")


def parse_raw_json_to_parquet(batch_size=50000) -> int:
    print("Starting to parse raw JSON file and prepare for Parquet conversion...")

    data_batch = []
    total_rows = 0
    writer = None

    try:
        with open(JSON_FILE_PATH, 'r') as file:
            for line_number, line in enumerate(file, start=1):
                # Read each JSON document as a string
                data_batch.append({'data': line.strip()})

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

        # Write any remaining rows in the last batch
        if data_batch:
            df_batch = pd.DataFrame(data_batch)
            table_batch = pa.Table.from_pandas(df_batch)
            if writer is None:
                writer = pq.ParquetWriter(
                    RAW_PARQUET_FILE_PATH, table_batch.schema)
            writer.write_table(table_batch)
            total_rows += len(data_batch)
            print(f"Final batch written. Total rows written to raw Parquet: {total_rows}")

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
                    json_obj = json.loads(line)
                    json_obj['data'] = line.strip()  # Add the JSON string

                    # Add the JSON document as a string
                    data_batch.append(json_obj)

                    # Once the batch reaches batch_size, write to Parquet
                    if len(data_batch) >= batch_size:
                        df_batch = pd.DataFrame(data_batch)
                        # Ensure all columns are included and in the correct order
                        df_batch = df_batch.reindex(columns=table_column_types.keys(), fill_value=None)
                        table_batch = pa.Table.from_pandas(df_batch)

                        # Initialize writer if it's the first batch
                        if writer is None:
                            writer = pq.ParquetWriter(
                                MATERIALIZED_PARQUET_FILE_PATH, table_batch.schema)

                        # Write the current batch to Parquet
                        writer.write_table(table_batch)
                        total_rows += len(data_batch)
                        data_batch = []  # Clear batch memory
                        print(f"Written {total_rows} rows to materialized Parquet so far...")

                except json.JSONDecodeError as e:
                    print(f"Error decoding JSON on line {line_number}: {e}")

        # Write any remaining rows in the last batch
        if data_batch:
            df_batch = pd.DataFrame(data_batch)
            df_batch = df_batch.reindex(columns=table_column_types.keys(), fill_value=None)
            table_batch = pa.Table.from_pandas(df_batch)
            if writer is None:
                writer = pq.ParquetWriter(
                    MATERIALIZED_PARQUET_FILE_PATH, table_batch.schema)
            writer.write_table(table_batch)
            total_rows += len(data_batch)
            print(f"Final batch written. Total rows written to materialized Parquet: {total_rows}")

    finally:
        if writer:
            writer.close()

    return total_rows, list(table_column_types.keys()), table_column_types


def insert_parquet_into_db(con: duckdb.DuckDBPyConnection, table_name: str, file_path: str, no_lines: int) -> float:
    print(f"Starting to insert data from {file_path} into DuckDB table {table_name}...")
    start_time = time.perf_counter()  # Start timing

    # Insert Parquet data into DuckDB
    con.execute("BEGIN TRANSACTION")
    con.execute(
        f"INSERT INTO {table_name} SELECT * FROM read_parquet('{file_path}')")
    con.execute("COMMIT")

    end_time = time.perf_counter()  # End timing
    total_time = end_time - start_time

    print(f"Inserted {no_lines} rows into DuckDB table {table_name}.")
    return total_time



def clean_up():
    for file_path in [RAW_PARQUET_FILE_PATH, MATERIALIZED_PARQUET_FILE_PATH]:
        if os.path.exists(file_path):
            os.remove(file_path)
            print(f"Deleted file {file_path}")
    print("Clean-up completed.")


if __name__ == '__main__':
    args = add_cmd_args()

    # Connect to or create the DuckDB instances
    raw_connection = duckdb.connect(RAW_DB_PATH)
    materialized_connection = duckdb.connect(MATERIALIZED_DB_PATH)

    # Clear and create required tables
    create_raw_data_db(con=raw_connection)

    # Parse the json into parquet and get all columns and types
    raw_total_rows = parse_raw_json_to_parquet()
    materialized_total_rows, all_columns, column_types = parse_materialized_json_to_parquet()

    # Create materialized data db after parsing to get all columns and types
    create_materialized_data_db(con=materialized_connection, all_columns=all_columns, column_types=column_types)

    env = args.environment
    tester = args.tester

    # Insert parquet into db and log results for raw data
    insert_time_raw = insert_parquet_into_db(
        con=raw_connection, table_name='tpch_data', file_path=RAW_PARQUET_FILE_PATH, no_lines=raw_total_rows)
    db_size_raw = os.path.getsize(RAW_DB_PATH)


    # Insert parquet into db and log results for materialized data
    insert_time_materialized = insert_parquet_into_db(
        con=materialized_connection, table_name='tpch_data', file_path=MATERIALIZED_PARQUET_FILE_PATH, no_lines=materialized_total_rows)
    db_size_materialized = os.path.getsize(MATERIALIZED_DB_PATH)


    # Delete created, unnecessary files
    clean_up()
