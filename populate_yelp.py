import json
import duckdb

# Connect to or create the 'yelp.db' database
connection = duckdb.connect('yelp.db')

# Create the table with a single JSON column if it doesn't already exist
create_table_query = '''
CREATE TABLE IF NOT EXISTS users (
    data JSON
);
'''
connection.execute(create_table_query)

# Open the JSON file and collect JSON objects into batches
batch_size = 10000
batch = []
batches = 1

with open('./data/yelp/yelp_academic_dataset_user.json', 'r') as file:
    for line in file:
        try:
            # Parse each line as a JSON object
            json_obj = json.loads(line)
            batch.append(json.dumps(json_obj))

            # When batch size is reached, insert the batch
            if len(batch) >= batch_size:
                connection.executemany("INSERT INTO users (data) VALUES (?)", [
                                       (item,) for item in batch])
                batch = []  # Reset the batch
                print(f"instered {batch_size*batches} documents")
                batches += 1
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON on line: {e}")

# Insert any remaining items in the last batch
if batch:
    connection.executemany("INSERT INTO users (data) VALUES (?)", [
                           (item,) for item in batch])

print("Data imported successfully.")
