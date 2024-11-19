import duckdb


raw_connection = duckdb.connect("raw_yelp.db")

#Run simple query on the duckdb database to check wether there is any data ther 
query =       """
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
    """
result = raw_connection.execute(query).fetchdf()
print(result)

