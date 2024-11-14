TPCH_ORIGINAL_DATA_PATH = './data'

import json
import os
import argparse
from datetime import datetime
from decimal import Decimal

# Define the mapping of table names to their column names and data types
table_definitions = {
    'customer': {
        'columns': [
            'c_custkey', 'c_name', 'c_address', 'c_nationkey',
            'c_phone', 'c_acctbal', 'c_mktsegment', 'c_comment'
        ],
        'types': [
            int,       # c_custkey
            str,       # c_name
            str,       # c_address
            int,       # c_nationkey
            str,       # c_phone
            float,     # c_acctbal
            str,       # c_mktsegment
            str        # c_comment
        ]
    },
    'lineitem': {
        'columns': [
            'l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber',
            'l_quantity', 'l_extendedprice', 'l_discount', 'l_tax',
            'l_returnflag', 'l_linestatus', 'l_shipdate', 'l_commitdate',
            'l_receiptdate', 'l_shipinstruct', 'l_shipmode', 'l_comment'
        ],
        'types': [
            int,       # l_orderkey
            int,       # l_partkey
            int,       # l_suppkey
            int,       # l_linenumber
            float,     # l_quantity
            float,     # l_extendedprice
            float,     # l_discount
            float,     # l_tax
            str,       # l_returnflag
            str,       # l_linestatus
            'date',    # l_shipdate
            'date',    # l_commitdate
            'date',    # l_receiptdate
            str,       # l_shipinstruct
            str,       # l_shipmode
            str        # l_comment
        ]
    },
    'nation': {
        'columns': [
            'n_nationkey', 'n_name', 'n_regionkey', 'n_comment'
        ],
        'types': [
            int,       # n_nationkey
            str,       # n_name
            int,       # n_regionkey
            str        # n_comment
        ]
    },
    'orders': {
        'columns': [
            'o_orderkey', 'o_custkey', 'o_orderstatus', 'o_totalprice',
            'o_orderdate', 'o_orderpriority', 'o_clerk', 'o_shippriority',
            'o_comment'
        ],
        'types': [
            int,       # o_orderkey
            int,       # o_custkey
            str,       # o_orderstatus
            float,     # o_totalprice
            'date',    # o_orderdate
            str,       # o_orderpriority
            str,       # o_clerk
            int,       # o_shippriority
            str        # o_comment
        ]
    },
    'part': {
        'columns': [
            'p_partkey', 'p_name', 'p_mfgr', 'p_brand', 'p_type',
            'p_size', 'p_container', 'p_retailprice', 'p_comment'
        ],
        'types': [
            int,       # p_partkey
            str,       # p_name
            str,       # p_mfgr
            str,       # p_brand
            str,       # p_type
            int,       # p_size
            str,       # p_container
            float,     # p_retailprice
            str        # p_comment
        ]
    },
    'partsupp': {
        'columns': [
            'ps_partkey', 'ps_suppkey', 'ps_availqty', 'ps_supplycost',
            'ps_comment'
        ],
        'types': [
            int,       # ps_partkey
            int,       # ps_suppkey
            int,       # ps_availqty
            float,     # ps_supplycost
            str        # ps_comment
        ]
    },
    'region': {
        'columns': [
            'r_regionkey', 'r_name', 'r_comment'
        ],
        'types': [
            int,       # r_regionkey
            str,       # r_name
            str        # r_comment
        ]
    },
    'supplier': {
        'columns': [
            's_suppkey', 's_name', 's_address', 's_nationkey',
            's_phone', 's_acctbal', 's_comment'
        ],
        'types': [
            int,       # s_suppkey
            str,       # s_name
            str,       # s_address
            int,       # s_nationkey
            str,       # s_phone
            float,     # s_acctbal
            str        # s_comment
        ]
    }
}

# The list of tables to process
tables = list(table_definitions.keys())

# Define date format used in the data (e.g., 'YYYY-MM-DD')
date_format = '%Y-%m-%d'

def main(limit_rows):
    output_file = TPCH_ORIGINAL_DATA_PATH + '/' + 'tpch_json.json'

    # Open the output file in write mode
    with open(output_file, 'w') as outfile:
        # Process each table
        for table in tables:
            columns = table_definitions[table]['columns']
            types = table_definitions[table]['types']
            filename = f'{table}.tbl'

            filePath = TPCH_ORIGINAL_DATA_PATH + "/" + filename

            # Check if the .tbl file exists
            if not os.path.exists(filePath):
                print(f'File {filePath} does not exist. Skipping table {table}.')
                continue

            row_count = 0  # Initialize row counter

            # Open the .tbl file
            with open(filePath, 'r') as infile:
                for line_number, line in enumerate(infile, start=1):
                    # Break if limit is reached
                    if limit_rows and row_count >= 10:
                        break

                    # Strip trailing whitespace and delimiter
                    line = line.strip()
                    if not line:
                        continue  # Skip empty lines

                    # Split the line by '|' and remove the last empty element
                    values = line.split('|')[:-1]

                    # Check for correct number of columns
                    if len(values) != len(columns):
                        print(f'Warning: Line {line_number} in {filename} has {len(values)} values but expected {len(columns)}. Skipping this line.')
                        continue

                    # Convert values to appropriate data types
                    converted_values = []
                    for i, (value, data_type) in enumerate(zip(values, types)):
                        try:
                            if data_type == int:
                                converted_values.append(int(value))
                            elif data_type == float:
                                converted_values.append(float(value))
                            elif data_type == 'date':
                                # Convert date string to ISO format
                                date_obj = datetime.strptime(value, date_format)
                                converted_values.append(date_obj.strftime('%Y-%m-%d'))
                            else:  # Assume string
                                converted_values.append(value)
                        except ValueError as e:
                            print(f'Warning: Type conversion error on table {table}, line {line_number}, column {columns[i]}: {e}. Setting value to None.')
                            converted_values.append(None)  # or handle as appropriate

                    # Map column names to their respective converted values
                    row = dict(zip(columns, converted_values))

                    # Write the JSON document to the output file
                    json.dump(row, outfile)
                    outfile.write('\n')

                    row_count += 1  # Increment row counter

    print(f'Conversion completed. JSON data is written to {output_file}.')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Convert TPC-H .tbl files to JSON.')
    parser.add_argument('--limit', action='store_true',
                        help='Limit output to first 10 rows from each table.')
    args = parser.parse_args()

    main(limit_rows=args.limit)
