import duckdb
import time
from datetime import datetime

RAW_DB_PATH = 'raw_yelp.db'
MATERIALIZED_DB_PATH = 'materialized_yelp.db'
RESULTS_DB_PATH = 'test_results.db'
RESULT_FILE_PATH = 'last_yelp_performance_test.txt'


RAW_QUERIES = [
    """
    SELECT COUNT(CAST(u.data->>'user_id' AS BIGINT)) AS cnt, CAST(u.data->>'user_id' AS BIGINT) AS uid, u.data->>'name', CAST(u.data->>'average_stars' AS FLOAT) AS avg_stars
    FROM users u, users r
    WHERE CAST(r.data->>'user_id' AS BIGINT) = CAST(u.data->>'user_id' AS BIGINT)
      AND u.data->>'name' IS NOT NULL
      AND CAST(u.data->>'average_stars' AS FLOAT) IS NOT NULL
      AND CAST(r.data->>'review_id' AS BIGINT) IS NOT NULL
    GROUP BY CAST(u.data->>'user_id' AS BIGINT), u.data->>'name', CAST(u.data->>'average_stars' AS FLOAT)
    ORDER BY cnt DESC
    LIMIT 10;
    """,
    """
    SELECT COUNT(DISTINCT CAST(r.data->>'user_id' AS BIGINT)) AS cnt, CAST(r.data->>'business_id' AS BIGINT) AS bid
    FROM users r
    WHERE CAST(r.data->>'date' AS DATE) > DATE '2017-01-01'
      AND CAST(r.data->>'date' AS DATE) < DATE '2018-01-01'
      AND CAST(r.data->>'review_id' AS INT) IS NOT NULL
    GROUP BY CAST(r.data->>'business_id' AS BIGINT)
    ORDER BY cnt DESC
    LIMIT 10;
    """,
    """
    WITH citycount (city, cnt, userid) AS (
        SELECT DISTINCT b.data->>'city' AS city, COUNT(*) AS cnt, CAST(r.data->>'user_id' AS BIGINT) AS userid
        FROM users b, users r
        WHERE CAST(r.data->>'business_id' AS BIGINT) = CAST(b.data->>'business_id' AS BIGINT)
          AND b.data->>'city' IS NOT NULL
          AND CAST(r.data->>'review_id' AS BIGINT) IS NOT NULL
        GROUP BY b.data->>'city', CAST(r.data->>'user_id' AS BIGINT)
    )
    SELECT c.city AS city, c.userid AS userid, u.data->>'name' AS username, c.cnt AS review_cnt
    FROM citycount c, users u, (SELECT c2.userid AS userid, c2.city AS city FROM citycount c2 WHERE c2.cnt = (SELECT MAX(c3.cnt) FROM citycount c3 WHERE c2.city = c3.city)) cj
    WHERE u.data->>'name' IS NOT NULL
      AND CAST(u.data->>'average_stars' AS FLOAT) IS NOT NULL
      AND c.userid = cj.userid
      AND c.city = cj.city
      AND c.userid = u.data->>'user_id'
    ORDER BY c.cnt DESC
    LIMIT 10;
    """,
    """
    SELECT COUNT(*) AS cnt, CAST(r.data->>'stars' AS FLOAT) AS stars
    FROM users r
    WHERE CAST(r.data->>'review_id' AS INT) IS NOT NULL
    GROUP BY CAST(r.data->>'stars' AS FLOAT)
    ORDER BY stars DESC;
    """,
    """
    SELECT b.data->>'city' AS city, COUNT(CAST(r.data->>'review_id' AS BIGINT)) / COUNT(DISTINCT CAST(r.data->>'business_id' AS BIGINT)) AS avg_reviews_per_business, COUNT(CAST(r.data->>'review_id' AS BIGINT)) AS reviews, COUNT(DISTINCT CAST(r.data->>'business_id' AS BIGINT)) AS businesses
    FROM users b, users r
    WHERE CAST(r.data->>'business_id' AS BIGINT) = CAST(b.data->>'business_id' AS BIGINT)
      AND b.data->>'city' IS NOT NULL
      AND CAST(r.data->>'review_id' AS BIGINT) IS NOT NULL
    GROUP BY b.data->>'city'
    ORDER BY avg_reviews_per_business DESC
    LIMIT 100;
    """
]


MATERIALIZED_QUERIES = [
    # Query 1: Top 10 users with the most reviews
    """
    SELECT COUNT(u.user_id) AS cnt, u.user_id AS uid, u.name, u.average_stars AS avg_stars
    FROM users u, users r
    WHERE r.user_id = u.user_id
      AND u.name IS NOT NULL
      AND u.average_stars IS NOT NULL
      AND r.review_id IS NOT NULL
    GROUP BY u.user_id, u.name, u.average_stars
    ORDER BY cnt DESC
    LIMIT 10;
    """,

    # Query 2: Businesses with the most unique user reviews in 2017
    """
    SELECT COUNT(DISTINCT r.user_id) AS cnt, r.business_id AS bid
    FROM users r
    WHERE r.date > DATE '2017-01-01'
      AND r.date < DATE '2018-01-01'
      AND r.review_id IS NOT NULL
    GROUP BY r.business_id
    ORDER BY cnt DESC
    LIMIT 10;
    """,

    # Query 3: Power users in cities with the most reviews by a single user
    """
    WITH citycount (city, cnt, userid) AS (
        SELECT DISTINCT b.city AS city, COUNT(*) AS cnt, r.user_id AS userid
        FROM users b, users r
        WHERE r.business_id = b.business_id
          AND b.city IS NOT NULL
          AND r.review_id IS NOT NULL
        GROUP BY b.city, r.user_id
    )
    SELECT c.city AS city, c.userid AS userid, u.name AS username, c.cnt AS review_cnt
    FROM citycount c, users u, 
         (SELECT c2.userid AS userid, c2.city AS city 
          FROM citycount c2 
          WHERE c2.cnt = (SELECT MAX(c3.cnt) FROM citycount c3 WHERE c2.city = c3.city)) cj
    WHERE u.name IS NOT NULL
      AND u.average_stars IS NOT NULL
      AND c.userid = cj.userid
      AND c.city = cj.city
      AND c.userid = u.user_id
    ORDER BY c.cnt DESC
    LIMIT 10;
    """,

    # Query 4: Count of reviews by star rating
    """
    SELECT COUNT(*) AS cnt, r.stars
    FROM users r
    WHERE r.review_id IS NOT NULL
    GROUP BY r.stars
    ORDER BY stars DESC;
    """,

    # Query 5: Cities with the most reviews per business
    """
    SELECT b.city AS city, COUNT(r.review_id) / COUNT(DISTINCT r.business_id) AS avg_reviews_per_business, COUNT(r.review_id) AS reviews, COUNT(DISTINCT r.business_id) AS businesses
    FROM users b, users r
    WHERE r.business_id = b.business_id
      AND b.city IS NOT NULL
      AND r.review_id IS NOT NULL
    GROUP BY b.city
    ORDER BY avg_reviews_per_business DESC
    LIMIT 100;
    """
]


def create_results_db(con: duckdb.DuckDBPyConnection):
    create_performance_table_query = '''
    CREATE TABLE IF NOT EXISTS yelp_query_performance (
        run_id UUID DEFAULT uuid(),
        timestamp TIMESTAMP,
        q1_avg_time FLOAT,
        q2_avg_time FLOAT,
        q3_avg_time FLOAT,
        q4_avg_time FLOAT,
        q5_avg_time FLOAT,
        comment VARCHAR
    );
    '''

    con.execute(create_performance_table_query)
    print("Created or confirmed existence of yelp_query_performance table.")


def perform_tests(con: duckdb.DuckDBPyConnection, queries: list) -> list:
    # Initialize a list to store average execution times
    avg_times = []

    # Execute each query 5 times and calculate average time of last 4 runs
    for i, query in enumerate(queries, start=1):
        times = []  # Store individual execution times for the current query

        for _ in range(5):  # Execute the query 5 times
            start_time = time.perf_counter()
            con.execute(query)  # Execute the query
            end_time = time.perf_counter()
            execution_time = end_time - start_time
            times.append(execution_time)

        # Calculate the average time of the last 4 runs and store it
        avg_time = sum(times[1:]) / 4
        avg_times.append(avg_time)
        print(f"""Query {i} Average Execution Time (last 4 runs): {
              avg_time:.4f} seconds""")
    return avg_times


def log_results(con: duckdb.DuckDBPyConnection, times: list, comment: str):
    con.execute('''
      INSERT INTO yelp_query_performance (timestamp, q1_avg_time, q2_avg_time, q3_avg_time, q4_avg_time, q5_avg_time, comment)
      VALUES (?, ?, ?, ?, ?, ?, ?)
  ''', (datetime.now(), *times, comment))


def write_results_to_file(test_type: str, times: list):
    with open(RESULT_FILE_PATH, 'a') as file:
        file.write(f"______________ {test_type} ______________\n")
        for i, time in enumerate(times, start=1):
            file.write(f"Query {i} Average Execution Time (last 4 runs): {
                       time:.4f} seconds\n")
        file.write("\n")


def write_queries_to_file():
    with open(RESULT_FILE_PATH, 'a') as file:
        file.write("______________QUERIES______________\n")
        file.write("RAW_QUERIES:\n")
        for query in RAW_QUERIES:
            file.write(query + "\n\n")
        file.write("MATERIALIZED_QUERIES:\n")
        for query in MATERIALIZED_QUERIES:
            file.write(query + "\n\n")


# Connect to the DuckDB instances
raw_connection = duckdb.connect(RAW_DB_PATH)
materialized_connection = duckdb.connect(MATERIALIZED_DB_PATH)
results_connection = duckdb.connect(RESULTS_DB_PATH)

# Create results table if not exists
create_results_db(con=results_connection)

# Perform tests
raw_avg_times = perform_tests(
    con=raw_connection, queries=RAW_QUERIES)
materialized_avg_times = perform_tests(
    con=materialized_connection, queries=MATERIALIZED_QUERIES)

# Log the results
log_results(con=results_connection, times=raw_avg_times,
            comment='Insert of raw json data')
log_results(con=results_connection, times=materialized_avg_times,
            comment='Insert of materialized json data')

# Write results to text file
with open(RESULT_FILE_PATH, 'w') as f:
    f.write("")  # Clear previous content

write_results_to_file("RAW", raw_avg_times)
write_results_to_file("MATERIALIZED", materialized_avg_times)

# Append the queries to the results file
write_queries_to_file()
