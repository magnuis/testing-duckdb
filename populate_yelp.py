import json
import duckdb

connection = duckdb.connect('yelp.db')

# Create table if it does not exist
create_table_query = '''
CREATE TABLE IF NOT EXISTS users (
    data json
    );
'''

connection.execute(create_table_query)


def import_json(obj):
    # Convert JSON object to a JSON string
    import_json_query = '''
    INSERT INTO users (data) VALUES (?);
    '''
    connection.execute(import_json_query, [json.dumps(obj)])


with open('./data/yelp/yelp_academic_dataset_user.json', 'r') as file:
    for line in file:
        try:
            json_obj = json.loads(line)
            import_json(json_obj)
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON on line: {e}")
