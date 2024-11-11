import os
import bz2
import argparse
import json
from collections import defaultdict


def decompress_bz2_file(path: str):
    decompressed_file_path = path[:-4]

    with bz2.open(path, 'rb') as compressed_file:
        with open(decompressed_file_path, 'wb') as decompressed_file:
            decompressed_file.write(compressed_file.read())


def infer_schema(json_obj, schema):
    """Recursively infers the schema of a JSON object and updates the schema dictionary."""
    if isinstance(json_obj, dict):
        for key, value in json_obj.items():
            value_type = type(value).__name__
            if key not in schema:
                schema[key] = defaultdict(int)
            schema[key][value_type] += 1
            # Recurse into nested objects or arrays
            if isinstance(value, (dict, list)):
                if "children" not in schema[key]:
                    schema[key]["children"] = {}
                infer_schema(value, schema[key]["children"])
    elif isinstance(json_obj, list):
        for item in json_obj:
            infer_schema(item, schema)


# Set up argument parsing for topdir
parser = argparse.ArgumentParser(
    description="Decompress bz2 files in specified directory.")
parser.add_argument('topdir', type=str,
                    help="Top directory containing subdirectories with bz2 files")
args = parser.parse_args()
top_dir = args.topdir

# Count the number of subdirectories in topdir
subdir_count = sum([1 for entry in os.scandir(top_dir) if entry.is_dir()])

finished = 0

fields = defaultdict(lambda: defaultdict(int))

total_files = sum(
    len([file for file in files if file.endswith('bz2')])
    for _, _, files in os.walk(top_dir)
)


for dirpath, _, files in os.walk(top_dir):

    for file_name in files:
        if file_name.endswith('bz2'):
            file_path = os.path.join(dirpath, file_name)
            decompress_bz2_file(path=file_path)
            # print(f"Decompress: {file_path}")
            os.remove(file_path)
            decompressed_file_path = file_path[:-4]

            with open(decompressed_file_path, 'r') as file:
                for line_number, line in enumerate(file, start=1):
                    try:
                        json_obj = json.loads(line)
                        infer_schema(json_obj, fields)
                    except json.JSONDecodeError as e:
                        print(f"""Error decoding JSON on line {
                              line_number}: {e}""")

        finished += 1
        print(f"Finished with {finished}/{total_files} dirs")

# Print or process the inferred schema
for field, types in fields.items():
    print(f"Field: {field}, Types: {dict(types)}")

with open('twitter_schema.json', 'w') as output_file:
    # Convert defaultdict to a regular dict for JSON serialization
    json.dump({k: dict(v) for k, v in fields.items()}, output_file, indent=4)
