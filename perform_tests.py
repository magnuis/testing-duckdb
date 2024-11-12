import time
from datetime import datetime
import pandas as pd
import duckdb
import argparse

# from queries.twitter_queries import
from queries.yelp_queries import RAW_YELP_QUERIES, MATERIALIZED_YELP_QUERIES
from queries.twitter_queries import RAW_TWITTER_QUERIES, MATERIALIZED_TWITTER_QUERIES
from queries.tpch_queries import RAW_TPCH_QUERIES, MATERIALIZED_TPCH_QUERIES

# Paths and queries for different datasets
DATASETS = {
    "twitter": {
        "raw_db_path": "raw_twitter.db",
        "materialized_db_path": "materialized_twitter.db",
        "raw_results_path": "twitter_raw_results.csv",
        "materialized_results_path": "twitter_materialized_results.csv",
        "raw_queries": RAW_TWITTER_QUERIES,
        "materialized_queries": MATERIALIZED_TWITTER_QUERIES,
    },
    "yelp": {
        "raw_db_path": "raw_yelp.db",
        "materialized_db_path": "materialized_yelp.db",
        "raw_results_path": "yelp_raw_results.csv",
        "materialized_results_path": "yelp_materialized_results.csv",
        "raw_queries": RAW_YELP_QUERIES,
        "materialized_queries": RAW_YELP_QUERIES,
    },
    "tpc-h": {
        "raw_db_path": "raw_tpch.db",
        "materialized_db_path": "materialized_tpch.db",
        "raw_results_path": "tpch_raw_results.csv",
        "materialized_results_path": "tpch_materialized_results.csv",
        "raw_queries": RAW_TPCH_QUERIES,
        "materialized_queries": MATERIALIZED_TPCH_QUERIES,
    }
}

DF_COL_NAMES = [
    'Query',
    'Avg (last 4 runs)',
    'Iteration 0',
    'Iteration 1',
    'Iteration 2',
    'Iteration 3',
    'Iteration 4',
    'Created At',
    'Test run no.',
]


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


def _results_dfs(raw_results_path, materialized_results_path):
    try:
        raw_results = pd.read_csv(raw_results_path)
        raw_run_no = raw_results['Test run no.'].max() + 1

    except Exception:
        raw_results = pd.DataFrame(columns=DF_COL_NAMES)
        raw_run_no = 1

    try:
        materialized_results = pd.read_csv(materialized_results_path)
        materialized_run_no = materialized_results['Test run no.'].max() + 1

    except Exception:
        materialized_results = pd.DataFrame(columns=DF_COL_NAMES)
        materialized_run_no = 1

    test_run_no = max(raw_run_no, materialized_run_no)

    return raw_results, materialized_results, test_run_no


def perform_tests(
        con: duckdb.DuckDBPyConnection,
        queries: list,
        run_no: int,
        test_time: datetime,
) -> pd.DataFrame:
    '''Perform the Twitter tests'''
    # Execute each query 5 times and calculate average time of last 4 runs

    results_df = pd.DataFrame(columns=DF_COL_NAMES)

    for i, query in enumerate(queries, start=1):
        df_row = {
            "Query": i,
            'Created At': test_time,
            'Test run no.': run_no,
        }
        execution_times = []

        for j in range(5):  # Execute the query 5 times
            start_time = time.perf_counter()
            con.execute(query)  # Execute the query
            end_time = time.perf_counter()
            execution_time = end_time - start_time
            execution_times.append(execution_time)
            df_row[f"Iteration {j}"] = round(execution_time, 3)

        # Calculate the average time of the last 4 runs and store it
        avg_time = round(sum(execution_times[1:]) / 4, 3)
        df_row['Avg (last 4 runs)'] = avg_time

        temp_df = pd.DataFrame([df_row])

        results_df = pd.concat([results_df, temp_df], ignore_index=True)
        print(f"""Query {i} Average Execution Time (last 4 runs): {
              avg_time:.4f} seconds""")
    return results_df


def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description="Run performance tests on different datasets.")
    parser.add_argument("dataset", nargs="?", default="all", choices=["tpc-h", "yelp", "twitter", "all"],
                        help="The dataset to run tests on (tpc-h, yelp, twitter, or all)")
    args = parser.parse_args()

    datasets_to_test = DATASETS.keys() if args.dataset == "all" else [
        args.dataset]

    for dataset in datasets_to_test:
        config = DATASETS[dataset]

        raw_results_path = config["raw_results_path"]
        raw_db_path = config["raw_db_path"]
        materialized_results_path = config["materialized_results_path"]
        materialized_db_path = config["materialized_db_path"]
        raw_queries = config["raw_queries"]
        materialized_queries = config["materialized_queries"]

        raw_connection = duckdb.connect(raw_db_path)
        materialized_connection = duckdb.connect(materialized_db_path)

        # Load results dataframes and determine run number
        raw_df, materialized_df, run_no = _results_dfs(
            raw_results_path=raw_results_path, materialized_results_path=materialized_results_path)

        test_time = datetime.now()

        # Perform and log tests for raw data
        raw_results = perform_tests(
            con=raw_connection, queries=raw_queries, run_no=run_no, test_time=test_time)
        raw_df = pd.concat([raw_df, raw_results], ignore_index=True)
        raw_df.to_csv(raw_results_path)
        raw_results.to_csv('last_' + raw_results_path)

        # Perform and log tests for materialized data
        materialized_results = perform_tests(
            con=materialized_connection, queries=materialized_queries, run_no=run_no, test_time=test_time)
        materialized_df = pd.concat(
            [materialized_df, materialized_results], ignore_index=True)
        materialized_df.to_csv(materialized_results_path)
        materialized_results.to_csv(
            'last_' + materialized_results_path)


if __name__ == "__main__":
    main()
