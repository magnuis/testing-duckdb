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

RAW_DB_PATH = './db/raw_tpch.db'
MATERIALIZED_DB_PATH = './db/materialized_tpch.db'

RAW_PARQUET_FILE_PATH = './db/tpch_raw_json.parquet'
MATERIALIZED_PARQUET_FILE_PATH = './db/tpch_materialized_json.parquet'

# Table definitions with explicit types
table_column_types = {
    'c_custkey': 'BIGINT', 'c_name': 'VARCHAR', 'c_address': 'VARCHAR', 
    'c_nationkey': 'BIGINT', 'c_phone': 'VARCHAR', 'c_acctbal': 'DOUBLE',
    'c_mktsegment': 'VARCHAR', 'c_comment': 'VARCHAR',
    
    'l_orderkey': 'BIGINT', 'l_partkey': 'BIGINT', 'l_suppkey': 'BIGINT',
    'l_linenumber': 'BIGINT', 'l_quantity': 'DOUBLE', 'l_extendedprice': 'DOUBLE',
    'l_discount': 'DOUBLE', 'l_tax': 'DOUBLE', 'l_returnflag': 'VARCHAR',
    'l_linestatus': 'VARCHAR', 'l_shipdate': 'DATE', 'l_commitdate': 'DATE',
    'l_receiptdate': 'DATE', 'l_shipinstruct': 'VARCHAR', 'l_shipmode': 'VARCHAR', 'l_comment': 'VARCHAR',
    
    'n_nationkey': 'BIGINT', 'n_name': 'VARCHAR', 'n_regionkey': 'BIGINT', 'n_comment': 'VARCHAR',
    
    'o_orderkey': 'BIGINT', 'o_custkey': 'BIGINT', 'o_orderstatus': 'VARCHAR', 
    'o_totalprice': 'DOUBLE', 'o_orderdate': 'DATE', 'o_orderpriority': 'VARCHAR',
    'o_clerk': 'VARCHAR', 'o_shippriority': 'BIGINT', 'o_comment': 'VARCHAR',
    
    'p_partkey': 'BIGINT', 'p_name': 'VARCHAR', 'p_mfgr': 'VARCHAR', 'p_brand': 'VARCHAR',
    'p_type': 'VARCHAR', 'p_size': 'BIGINT', 'p_container': 'VARCHAR',
    'p_retailprice': 'DOUBLE', 'p_comment': 'VARCHAR',
    
    'ps_partkey': 'BIGINT', 'ps_suppkey': 'BIGINT', 'ps_availqty': 'BIGINT',
    'ps_supplycost': 'DOUBLE', 'ps_comment': 'VARCHAR',
    
    'r_regionkey': 'BIGINT', 'r_name': 'VARCHAR', 'r_comment': 'VARCHAR',
    
    's_suppkey': 'BIGINT', 's_name': 'VARCHAR', 's_address': 'VARCHAR',
    's_nationkey': 'BIGINT', 's_phone': 'VARCHAR', 's_acctbal': 'DOUBLE', 's_comment': 'VARCHAR',
    
    'data': 'VARCHAR'  # Added as the JSON string data column
}

# List of columns to materialize
materialized_columns = [
    'c_acctbal', 'c_address', 'c_comment', 'c_custkey', 'c_mktsegment', 'c_name', 
    'c_nationkey', 'c_phone', 'l_commitdate', 'l_discount', 'l_extendedprice', 
    'l_linestatus', 'l_orderkey', 'l_partkey', 'l_quantity', 'l_receiptdate', 
    'l_returnflag', 'l_shipdate', 'l_shipinstruct', 'l_shipmode', 'l_suppkey', 
    'l_tax', 'n_name', 'n_nationkey', 'n_regionkey', 'o_comment', 'o_custkey', 
    'o_orderdate', 'o_orderkey', 'o_orderpriority', 'o_orderstatus', 
    'o_shippriority', 'o_totalprice', 'p_brand', 'p_container', 'p_mfgr', 
    'p_name', 'p_partkey', 'p_size', 'p_type', 'ps_availqty', 'ps_partkey', 
    'ps_suppkey', 'ps_supplycost', 'r_name', 'r_regionkey', 's_acctbal', 
    's_address', 's_comment', 's_name', 's_nationkey', 's_phone', 's_suppkey', 'data'
]

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
    # Drop the tpch table if it exists, to ensure a fresh start
    con.execute("DROP TABLE IF EXISTS tpch")

    # Create the tpch table with one data column
    con.execute("CREATE TABLE tpch (data JSON)")
    print("Created fresh tpch table for raw JSON data.")


def create_materialized_data_db(con: duckdb.DuckDBPyConnection, column_types):
    # Drop the tpch table if it exists, to ensure a fresh start
    con.execute("DROP TABLE IF EXISTS tpch")

    # Construct the CREATE TABLE query with only materialized columns
    create_table_query = "CREATE TABLE tpch ("
    create_table_query += ", ".join([
        f"{col} {column_types[col]}" for col in materialized_columns
    ])
    create_table_query += ");"

    con.execute(create_table_query)
    print("Created fresh tpch table for materialized JSON data with selected columns.")



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

    # Build the pyarrow schema and pandas dtype mapping
    fields = []
    pandas_dtypes = {}
    for col in materialized_columns:
        duckdb_type = table_column_types[col]
        if duckdb_type == 'BIGINT':
            pa_type = pa.int64()
            pandas_dtypes[col] = 'Int64'  # Pandas nullable integer
        elif duckdb_type == 'DOUBLE':
            pa_type = pa.float64()
            pandas_dtypes[col] = 'float64'
        elif duckdb_type == 'VARCHAR':
            pa_type = pa.string()
            pandas_dtypes[col] = 'string'  # Use 'string' dtype for better performance
        elif duckdb_type == 'DATE':
            pa_type = pa.date32()
            pandas_dtypes[col] = 'datetime64[ns]'
        else:
            pa_type = pa.string()
            pandas_dtypes[col] = 'string'
        fields.append(pa.field(col, pa_type))

    schema = pa.schema(fields)

    try:
        with open(JSON_FILE_PATH, 'r') as file:
            for line_number, line in enumerate(file, start=1):
                try:
                    json_obj = json.loads(line)
                    json_obj['data'] = line.strip()  # Add the JSON string

                    # Filter only materialized columns
                    filtered_obj = {col: json_obj.get(col) for col in materialized_columns}
                    data_batch.append(filtered_obj)

                    # Once the batch reaches batch_size, write to Parquet
                    if len(data_batch) >= batch_size:
                        df_batch = pd.DataFrame(data_batch)
                        df_batch = df_batch.reindex(columns=materialized_columns, fill_value=None)

                        # Enforce data types in pandas DataFrame
                        for col, dtype in pandas_dtypes.items():
                            if dtype == 'Int64':
                                df_batch[col] = pd.to_numeric(df_batch[col], errors='coerce').astype('Int64')
                            elif dtype == 'float64':
                                df_batch[col] = pd.to_numeric(df_batch[col], errors='coerce')
                            elif dtype == 'datetime64[ns]':
                                df_batch[col] = pd.to_datetime(df_batch[col], errors='coerce')
                            elif dtype == 'string':
                                df_batch[col] = df_batch[col].astype('string')
                            else:
                                df_batch[col] = df_batch[col].astype('object')

                        table_batch = pa.Table.from_pandas(df_batch, schema=schema, safe=False)

                        if writer is None:
                            writer = pq.ParquetWriter(
                                MATERIALIZED_PARQUET_FILE_PATH, schema)

                        writer.write_table(table_batch)
                        total_rows += len(data_batch)
                        data_batch = []  # Clear batch memory
                        print(f"Written {total_rows} rows to materialized Parquet so far...")

                except json.JSONDecodeError as e:
                    print(f"Error decoding JSON on line {line_number}: {e}")

        # Write any remaining rows in the last batch
        if data_batch:
            df_batch = pd.DataFrame(data_batch)
            df_batch = df_batch.reindex(columns=materialized_columns, fill_value=None)

            # Enforce data types in pandas DataFrame
            for col, dtype in pandas_dtypes.items():
                if dtype == 'Int64':
                    df_batch[col] = pd.to_numeric(df_batch[col], errors='coerce').astype('Int64')
                elif dtype == 'float64':
                    df_batch[col] = pd.to_numeric(df_batch[col], errors='coerce')
                elif dtype == 'datetime64[ns]':
                    df_batch[col] = pd.to_datetime(df_batch[col], errors='coerce')
                elif dtype == 'string':
                    df_batch[col] = df_batch[col].astype('string')
                else:
                    df_batch[col] = df_batch[col].astype('object')

            table_batch = pa.Table.from_pandas(df_batch, schema=schema, safe=False)
            if writer is None:
                writer = pq.ParquetWriter(
                    MATERIALIZED_PARQUET_FILE_PATH, schema)
            writer.write_table(table_batch)
            total_rows += len(data_batch)
            print(f"Final batch written. Total rows written to materialized Parquet: {total_rows}")

    finally:
        if writer:
            writer.close()

    return total_rows, materialized_columns, table_column_types


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
    create_materialized_data_db(con=materialized_connection, column_types=column_types)

    env = args.environment
    tester = args.tester

    # Insert parquet into db and log results for raw data
    insert_time_raw = insert_parquet_into_db(
        con=raw_connection, table_name='tpch', file_path=RAW_PARQUET_FILE_PATH, no_lines=raw_total_rows)
    db_size_raw = os.path.getsize(RAW_DB_PATH)


    # Insert parquet into db and log results for materialized data
    insert_time_materialized = insert_parquet_into_db(
        con=materialized_connection, table_name='tpch', file_path=MATERIALIZED_PARQUET_FILE_PATH, no_lines=materialized_total_rows)
    db_size_materialized = os.path.getsize(MATERIALIZED_DB_PATH)


    # Delete created, unnecessary files
    clean_up()
