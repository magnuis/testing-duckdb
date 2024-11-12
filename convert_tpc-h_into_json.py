

TPCH_ORIGINAL_DATA_PATH = './data/tpc-h'

import json
import os
import argparse
from datetime import datetime
from decimal import Decimal

# Define the mapping of table names to their column names and data types
table_definitions = {
    'customer': {
        'columns': [
            'C_CUSTKEY', 'C_NAME', 'C_ADDRESS', 'C_NATIONKEY',
            'C_PHONE', 'C_ACCTBAL', 'C_MKTSEGMENT', 'C_COMMENT'
        ],
        'types': [
            int,       # C_CUSTKEY
            str,       # C_NAME
            str,       # C_ADDRESS
            int,       # C_NATIONKEY
            str,       # C_PHONE
            float,     # C_ACCTBAL
            str,       # C_MKTSEGMENT
            str        # C_COMMENT
        ]
    },
    'lineitem': {
        'columns': [
            'L_ORDERKEY', 'L_PARTKEY', 'L_SUPPKEY', 'L_LINENUMBER',
            'L_QUANTITY', 'L_EXTENDEDPRICE', 'L_DISCOUNT', 'L_TAX',
            'L_RETURNFLAG', 'L_LINESTATUS', 'L_SHIPDATE', 'L_COMMITDATE',
            'L_RECEIPTDATE', 'L_SHIPINSTRUCT', 'L_SHIPMODE', 'L_COMMENT'
        ],
        'types': [
            int,       # L_ORDERKEY
            int,       # L_PARTKEY
            int,       # L_SUPPKEY
            int,       # L_LINENUMBER
            float,     # L_QUANTITY
            float,     # L_EXTENDEDPRICE
            float,     # L_DISCOUNT
            float,     # L_TAX
            str,       # L_RETURNFLAG
            str,       # L_LINESTATUS
            'date',    # L_SHIPDATE
            'date',    # L_COMMITDATE
            'date',    # L_RECEIPTDATE
            str,       # L_SHIPINSTRUCT
            str,       # L_SHIPMODE
            str        # L_COMMENT
        ]
    },
    'nation': {
        'columns': [
            'N_NATIONKEY', 'N_NAME', 'N_REGIONKEY', 'N_COMMENT'
        ],
        'types': [
            int,       # N_NATIONKEY
            str,       # N_NAME
            int,       # N_REGIONKEY
            str        # N_COMMENT
        ]
    },
    'orders': {
        'columns': [
            'O_ORDERKEY', 'O_CUSTKEY', 'O_ORDERSTATUS', 'O_TOTALPRICE',
            'O_ORDERDATE', 'O_ORDERPRIORITY', 'O_CLERK', 'O_SHIPPRIORITY',
            'O_COMMENT'
        ],
        'types': [
            int,       # O_ORDERKEY
            int,       # O_CUSTKEY
            str,       # O_ORDERSTATUS
            float,     # O_TOTALPRICE
            'date',    # O_ORDERDATE
            str,       # O_ORDERPRIORITY
            str,       # O_CLERK
            int,       # O_SHIPPRIORITY
            str        # O_COMMENT
        ]
    },
    'part': {
        'columns': [
            'P_PARTKEY', 'P_NAME', 'P_MFGR', 'P_BRAND', 'P_TYPE',
            'P_SIZE', 'P_CONTAINER', 'P_RETAILPRICE', 'P_COMMENT'
        ],
        'types': [
            int,       # P_PARTKEY
            str,       # P_NAME
            str,       # P_MFGR
            str,       # P_BRAND
            str,       # P_TYPE
            int,       # P_SIZE
            str,       # P_CONTAINER
            float,     # P_RETAILPRICE
            str        # P_COMMENT
        ]
    },
    'partsupp': {
        'columns': [
            'PS_PARTKEY', 'PS_SUPPKEY', 'PS_AVAILQTY', 'PS_SUPPLYCOST',
            'PS_COMMENT'
        ],
        'types': [
            int,       # PS_PARTKEY
            int,       # PS_SUPPKEY
            int,       # PS_AVAILQTY
            float,     # PS_SUPPLYCOST
            str        # PS_COMMENT
        ]
    },
    'region': {
        'columns': [
            'R_REGIONKEY', 'R_NAME', 'R_COMMENT'
        ],
        'types': [
            int,       # R_REGIONKEY
            str,       # R_NAME
            str        # R_COMMENT
        ]
    },
    'supplier': {
        'columns': [
            'S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_NATIONKEY',
            'S_PHONE', 'S_ACCTBAL', 'S_COMMENT'
        ],
        'types': [
            int,       # S_SUPPKEY
            str,       # S_NAME
            str,       # S_ADDRESS
            int,       # S_NATIONKEY
            str,       # S_PHONE
            float,     # S_ACCTBAL
            str        # S_COMMENT
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

                    # Optional: Include the table name in the JSON document
                    row['table'] = table

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
