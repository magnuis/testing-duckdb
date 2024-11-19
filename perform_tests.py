import time
from datetime import datetime
import pandas as pd
import numpy as np
import duckdb
import argparse

# from queries.twitter_queries import
from queries.yelp_queries import RAW_YELP_QUERIES, MATERIALIZED_YELP_QUERIES
from queries.twitter_queries import RAW_TWITTER_QUERIES, MATERIALIZED_TWITTER_QUERIES
from tpch.tpch_queries import RAW_TPCH_QUERIES, MATERIALIZED_TPCH_QUERIES

# Paths and queries for different datasets
DATASETS = {
    "twitter": {
        "raw_db_path": "raw_twitter.db",
        "materialized_db_path": "materialized_twitter.db",
        "raw_results_path": "twitter_raw_results.csv",
        "last_raw_results_path": "last_twitter_raw_results.csv",
        "materialized_results_path": "twitter_materialized_results.csv",
        "last_materialized_results_path": "last_twitter_materialized_results.csv",
        "raw_queries": RAW_TWITTER_QUERIES,
        "materialized_queries": MATERIALIZED_TWITTER_QUERIES,
    },
    "yelp": {
        "raw_db_path": "raw_yelp.db",
        "materialized_db_path": "materialized_yelp.db",
        "raw_results_path": "yelp_raw_results.csv",
        "last_raw_results_path": "last_yelp_raw_results.csv",
        "materialized_results_path": "yelp_materialized_results.csv",
        "last_materialized_results_path": "last_yelp_materialized_results.csv",
        "raw_queries": RAW_YELP_QUERIES,
        "materialized_queries": MATERIALIZED_YELP_QUERIES,
    },
    "tpc-h": {
        "raw_db_path": "./tpch/db/raw_tpch.db",
        "materialized_db_path": "./tpch/db/materialized_tpch.db",
        "raw_results_path": "./tpch/test_output/tpch_raw_results.csv",
        "last_raw_results_path": "./tpch/test_output/last_tpch_raw_results.csv",
        "materialized_results_path": "./tpch/test_output/tpch_materialized_results.csv",
        "last_materialized_results_path": "./tpch/test_output/last_tpch_materialized_results.csv",
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
) -> tuple[pd.DataFrame, list]:
    '''Perform the tests and collect results from the first execution'''
    # Execute each query 5 times and calculate average time of last 4 runs
    results_df = pd.DataFrame(columns=DF_COL_NAMES)
    query_results = []  # List to store results from the first execution

    for i, query in enumerate(queries, start=1):

        if i != 1:
            continue

        
        df_row = {
            "Query": i,
            'Created At': test_time,
            'Test run no.': run_no,
        }
        execution_times = []
        first_run_result = None

        iterations = 1

        for j in range(iterations):  # Execute the query 5 times
            start_time = time.perf_counter()
            result = con.execute(query).fetchdf()  # Execute the query and fetch result
            end_time = time.perf_counter()
            execution_time = end_time - start_time
            execution_times.append(execution_time)
            df_row[f"Iteration {j}"] = round(execution_time, 3)

            if j == 0:
                # Store the result from the first execution
                # print(result)
                first_run_result = result.copy()
            # if j==1:
            #     print(query)
            #     print(first_run_result)
            #     print(result)
            #     print('---')
      


        # Collect the result from the first run
        query_results.append(first_run_result)

        # Calculate the average time of the last 4 runs and store it
        avg_time = -1
        # avg_time = sum(execution_times[1:]) / (iterations - 1)
        df_row['Avg (last 4 runs)'] = avg_time

        temp_df = pd.DataFrame([df_row])

        results_df = pd.concat([results_df, temp_df], ignore_index=True).reset_index(drop=True)
        print(f"""Query {i} Average Execution Time (last 4 runs): {
              avg_time:.4f} seconds""")
    return results_df, query_results

def compare_query_results(raw_query_results, materialized_query_results):
    '''Compare the results of raw and materialized queries'''
    for i, (raw_df, materialized_df) in enumerate(zip(raw_query_results, materialized_query_results), start=1):
        # Sort dataframes to ensure consistent ordering before comparison
        try:
            raw_df_sorted = raw_df.sort_values(by=raw_df.columns.tolist()).reset_index(drop=True)
            materialized_df_sorted = materialized_df.sort_values(by=materialized_df.columns.tolist()).reset_index(drop=True)
        except Exception as e:
            # If sorting fails, proceed without sorting
            raw_df_sorted = raw_df.reset_index(drop=True)
            materialized_df_sorted = materialized_df.reset_index(drop=True)
        
        equals = True
        for r_col, m_col in zip(raw_df_sorted.columns, materialized_df_sorted.columns):
            raw = raw_df_sorted[r_col]
            materialized = materialized_df_sorted[m_col]


            print(f"dtype: {raw.dtype}")

            if raw.dtype == np.float64:
                if not np.all(np.isclose(raw, materialized)):
                    print("NOT EQUAL float64 ROWS")
                    print(raw)
                    print(materialized)
                    equals = False
                    
                    break                    


            else:
                if not raw.equals(materialized):
                    print("NOT EQUAL non-float ROWS")
                    print(raw)
                    print(materialized)
                    equals = False
                    
                    break



        if equals:
            print(f"Query {i}: Results match.")

            # Output reults for both queries so I can compaere them
            # print(f"Raw Query {i} results:\n{raw_df_sorted}")
            # print(f"Materialized Query {i} results:\n{materialized_df_sorted}")


        else:
            print(f"Query {i}: Results do not match.")
            # Optionally, output the differences for debugging
            differences = pd.concat([raw_df_sorted, materialized_df_sorted]).drop_duplicates(keep=False)
            # print(f"Differences in Query {i} results:\n{differences}")

                        # Output reults for both queries so I can compaere them
            print(f"Raw Query {i} results:\n{raw_df_sorted}")
            print(f"Materialized Query {i} results:\n{materialized_df_sorted}")
            print('---')
            print(raw_df_sorted.info())
            print('---')
            print(materialized_df_sorted.info())
            print('---')

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
        last_raw_results_path = config["last_raw_results_path"]
        raw_db_path = config["raw_db_path"]
        materialized_results_path = config["materialized_results_path"]
        last_materialized_results_path = config["last_materialized_results_path"]
        materialized_db_path = config["materialized_db_path"]
        raw_queries = config["raw_queries"]
        materialized_queries = config["materialized_queries"]

        raw_connection = duckdb.connect(raw_db_path)
        materialized_connection = duckdb.connect(materialized_db_path)

        # Load results dataframes and determine run number
        raw_df, materialized_df, run_no = _results_dfs(
            raw_results_path=raw_results_path, materialized_results_path=materialized_results_path)

        test_time = datetime.now()

        # Perform and log tests for raw data, collect results
        raw_results_df, raw_query_results = perform_tests(
            con=raw_connection, queries=raw_queries, run_no=run_no, test_time=test_time)
        raw_df = pd.concat([raw_df, raw_results_df], ignore_index=True)
        raw_df.to_csv(raw_results_path, index=False)
        raw_results_df.to_csv(last_raw_results_path, index=False)

        # Perform and log tests for materialized data, collect results
        materialized_results_df, materialized_query_results = perform_tests(
            con=materialized_connection, queries=materialized_queries, run_no=run_no, test_time=test_time)
        materialized_df = pd.concat(
            [materialized_df, materialized_results_df], ignore_index=True)
        materialized_df.to_csv(materialized_results_path, index=False)
        materialized_results_df.to_csv(last_materialized_results_path, index=False)

        # Compare the results of raw and materialized queries
        print(f"\nComparing query results for dataset: {dataset}")
        compare_query_results(
            raw_query_results=raw_query_results,
            materialized_query_results=materialized_query_results
        )

if __name__ == "__main__":
    main()
