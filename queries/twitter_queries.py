RAW_TWITTER_QUERIES = [
    """
    SELECT COUNT(*) AS tweet_count
    FROM tweets
    WHERE data->>'lang' = 'en';
    """,
    """
    SELECT COUNT(*) AS tweet_count, data->>'source' AS source
    FROM tweets
    WHERE data->>'source' IS NOT NULL
    GROUP BY data->>'source'
    ORDER BY tweet_count DESC
    LIMIT 5;
    """,
    """
    SELECT COUNT(*) AS retweet_count, data1->>'user'->>'screen_name' AS original_user 
    FROM tweets t1, tweets t2 
    WHERE t1.data->>'retweeted_status'->>'id_str' = t2.data->>'id_str' 
    GROUP BY original_user 
    ORDER BY retweet_count 
    DESC LIMIT 10;
    """,
    """
    SELECT COUNT(*) AS reply_count, t1.data->>'in_reply_to_screen_name' AS replied_user 
    FROM tweets t1, tweets t2, tweets t3 
    WHERE t1.data->>'in_reply_to_status_id_str' = t2.data->>'id_str' 
    AND t2.data->>'in_reply_to_status_id_str' = t3.data->>'id_str' 
    GROUP BY replied_user 
    ORDER BY reply_count 
    DESC LIMIT 15;
    """,
    """
    WITH user_tweets AS ( 
        SELECT data->>'user'->>'id_str' AS user_id, COUNT(*) AS tweet_count 
        FROM tweets 
        GROUP BY user_id 
    ) 
    SELECT u.data->>'name' AS user_name, ut.tweet_count 
    FROM users u, user_tweets ut, tweets t 
    WHERE u.data->>'id_str' = ut.user_id 
    AND u.data->>'followers_count' > 1000 
    ORDER BY ut.tweet_count 
    DESC LIMIT 10;
    """,
    """
    WITH user_tweets AS ( 
        SELECT data->>'user'->>'id_str' AS user_id, COUNT(*) AS tweet_count 
        FROM tweets 
        GROUP BY user_id 
    ) 
    SELECT u.data->>'name' AS user_name, ut.tweet_count 
    FROM users u, user_tweets ut, tweets t 
    WHERE u.data->>'id_str' = ut.user_id 
    AND u.data->>'followers_count' > 1000 
    ORDER BY ut.tweet_count 
    DESC LIMIT 10;
    """,
    """
    WITH HashtagCounts AS (
        SELECT LOWER(h.data->>'text') AS hashtag, COUNT(*) AS hashtag_count
        FROM tweets t, jsonb_array_elements(t.data->'entities'->'hashtags') AS h
        GROUP BY LOWER(h.data->>'text')
        ORDER BY hashtag_count DESC
        LIMIT 10
    ),
    CooccurringHashtags AS (
        SELECT 
            LOWER(h1.data->>'text') AS primary_hashtag,
            LOWER(h2.data->>'text') AS cooccurring_hashtag,
            COUNT(*) AS cooccurrence_count
        FROM tweets t,
            jsonb_array_elements(t.data->'entities'->'hashtags') AS h1,
            jsonb_array_elements(t.data->'entities'->'hashtags') AS h2
        WHERE LOWER(h1.data->>'text') != LOWER(h2.data->>'text')
        GROUP BY primary_hashtag, cooccurring_hashtag
    )
    SELECT hc.hashtag AS primary_hashtag, ch.cooccurring_hashtag, ch.cooccurrence_count
    FROM HashtagCounts hc
    JOIN CooccurringHashtags ch
    ON hc.hashtag = ch.primary_hashtag
    ORDER BY hc.hashtag, ch.cooccurrence_count DESC
    LIMIT 50;
    """,
    """
    WITH GagaLikers AS (
        SELECT DISTINCT t.data->>'user'->>'id_str' AS user_id
        FROM tweets t
        WHERE LOWER(t.data->>'text') LIKE '%lady gaga%'
        OR EXISTS (
            SELECT 1
            FROM jsonb_array_elements(t.data->'entities'->'hashtags') AS h
            WHERE LOWER(h->>'text') = 'ladygaga'
        )
    ),
    UserHashtags AS (
        -- Collect hashtags used by those users
        SELECT LOWER(h.data->>'text') AS hashtag
        FROM tweets t, jsonb_array_elements(t.data->'entities'->'hashtags') AS h
        WHERE t.data->>'user'->>'id_str' IN (SELECT user_id FROM GagaLikers)
    )
    SELECT hashtag, COUNT(*) AS usage_count
    FROM UserHashtags
    GROUP BY hashtag
    ORDER BY usage_count DESC

    LIMIT 10;
    """,
    """
    WITH CovidLikers AS (
    SELECT DISTINCT t.data->>'user'->>'id_str' AS user_id
    FROM tweets t
    WHERE LOWER(t.data->>'text') LIKE '%covid-19%'
       OR EXISTS (
           SELECT 1
           FROM jsonb_array_elements(t.data->'entities'->'hashtags') AS h
           WHERE LOWER(h->>'text') IN ('covid19', 'covid-19')
       )
    ),
    TotalUsers AS (
        SELECT COUNT(DISTINCT t.data->>'user'->>'id_str') AS total_user_count
        FROM tweets t
    ),
    CovidUserCount AS (
        -- Count the number of users who have liked a COVID-19-related tweet
        SELECT COUNT(*) AS covid_user_count
        FROM CovidLikers
    )
    SELECT (cuc.covid_user_count::FLOAT / tu.total_user_count::FLOAT) * 100 AS percentage
    FROM CovidUserCount cuc, TotalUsers tu;
    """,
    """
    SELECT 
        t1.data->>'user'->>'screen_name' AS original_user,
        t2.data->>'user'->>'screen_name' AS retweeting_user,
        LOWER(h.data->>'text') AS common_hashtag,
        t2.data->>'source' AS retweet_source
    FROM 
        tweets t1
    JOIN 
        tweets t2 ON t1.data->>'id_str' = t2.data->>'retweeted_status'->>'id_str'
    JOIN 
        jsonb_array_elements(t2.data->'entities'->'hashtags') AS h ON TRUE
    JOIN 
        users u1 ON t1.data->>'user'->>'id_str' = u1.data->>'id_str'
    JOIN 
        users u2 ON t2.data->>'user'->>'id_str' = u2.data->>'id_str'
    WHERE 
        t1.data->>'user'->>'screen_name' IS NOT NULL
        AND t2.data->>'user'->>'screen_name' IS NOT NULL
        AND h.data->>'text' IS NOT NULL
    ORDER BY 
        common_hashtag, retweet_source
    LIMIT 20;
    """
]

MATERIALIZED_TWITTER_QUERIES = [
    """
    SELECT COUNT(*) AS tweet_count
    FROM tweets
    WHERE lang = 'en';
    """,
    """
    SELECT COUNT(*) AS tweet_count, source
    FROM tweets
    WHERE source IS NOT NULL
    GROUP BY source
    ORDER BY tweet_count DESC
    LIMIT 5;
    """,
    """
    SELECT COUNT(*) AS retweet_count, t1.screen_name AS original_user
    FROM tweets t1
    JOIN tweets t2 ON t1.retweeted_status_id = t2.id
    GROUP BY original_user
    ORDER BY retweet_count DESC
    LIMIT 10;
    """,
    """
    SELECT COUNT(*) AS reply_count, t1.in_reply_to_screen_name AS replied_user
    FROM tweets t1
    JOIN tweets t2 ON t1.in_reply_to_status_id = t2.id
    JOIN tweets t3 ON t2.in_reply_to_status_id = t3.id
    GROUP BY replied_user
    ORDER BY reply_count DESC
    LIMIT 15;
    """,
    """
    WITH user_tweets AS (
        SELECT user_id, COUNT(*) AS tweet_count
        FROM tweets
        GROUP BY user_id
    )
    SELECT u.name AS user_name, ut.tweet_count
    FROM users u
    JOIN user_tweets ut ON u.id = ut.user_id
    WHERE u.followers_count > 1000
    ORDER BY ut.tweet_count DESC
    LIMIT 10;
    """,
    """
    WITH user_tweets AS (
        SELECT user_id, COUNT(*) AS tweet_count
        FROM tweets
        GROUP BY user_id
    )
    SELECT u.name AS user_name, ut.tweet_count
    FROM users u
    JOIN user_tweets ut ON u.id = ut.user_id
    WHERE u.followers_count > 1000
    ORDER BY ut.tweet_count DESC
    LIMIT 10;
    """,
    """
    WITH HashtagCounts AS (
        SELECT LOWER(h.text) AS hashtag, COUNT(*) AS hashtag_count
        FROM tweets t
        JOIN UNNEST(t.hashtags) AS h
        GROUP BY LOWER(h.text)
        ORDER BY hashtag_count DESC
        LIMIT 10
    ),
    CooccurringHashtags AS (
        SELECT 
            LOWER(h1.text) AS primary_hashtag,
            LOWER(h2.text) AS cooccurring_hashtag,
            COUNT(*) AS cooccurrence_count
        FROM tweets t
        JOIN UNNEST(t.hashtags) AS h1
        JOIN UNNEST(t.hashtags) AS h2
        WHERE LOWER(h1.text) != LOWER(h2.text)
        GROUP BY primary_hashtag, cooccurring_hashtag
    )
    SELECT hc.hashtag AS primary_hashtag, ch.cooccurring_hashtag, ch.cooccurrence_count
    FROM HashtagCounts hc
    JOIN CooccurringHashtags ch ON hc.hashtag = ch.primary_hashtag
    ORDER BY hc.hashtag, ch.cooccurrence_count DESC
    LIMIT 50;
    """,
    """
    WITH GagaLikers AS (
        SELECT DISTINCT user_id
        FROM tweets
        WHERE LOWER(text) LIKE '%lady gaga%'
        OR EXISTS (
            SELECT 1
            FROM UNNEST(hashtags) AS h
            WHERE LOWER(h) = 'ladygaga'
        )
    ),
    UserHashtags AS (
        SELECT LOWER(h.text) AS hashtag
        FROM tweets
        JOIN UNNEST(hashtags) AS h
        WHERE user_id IN (SELECT user_id FROM GagaLikers)
    )
    SELECT hashtag, COUNT(*) AS usage_count
    FROM UserHashtags
    GROUP BY hashtag
    ORDER BY usage_count DESC
    LIMIT 10;
    """,
    """
    WITH CovidLikers AS (
        SELECT DISTINCT user_id
        FROM tweets
        WHERE LOWER(text) LIKE '%covid-19%'
        OR EXISTS (
            SELECT 1
            FROM UNNEST(hashtags) AS h
            WHERE LOWER(h) IN ('covid19', 'covid-19')
        )
    ),
    TotalUsers AS (
        SELECT COUNT(DISTINCT user_id) AS total_user_count
        FROM tweets
    ),
    CovidUserCount AS (
        SELECT COUNT(*) AS covid_user_count
        FROM CovidLikers
    )
    SELECT (cuc.covid_user_count::FLOAT / tu.total_user_count::FLOAT) * 100 AS percentage
    FROM CovidUserCount cuc, TotalUsers tu;
    """,
    """
    SELECT 
        t1.screen_name AS original_user,
        t2.screen_name AS retweeting_user,
        LOWER(h.text) AS common_hashtag,
        t2.source AS retweet_source
    FROM tweets t1
    JOIN tweets t2 ON t1.id = t2.retweeted_status_id
    JOIN UNNEST(t2.hashtags) AS h
    JOIN users u1 ON t1.user_id = u1.id
    JOIN users u2 ON t2.user_id = u2.id
    WHERE t1.screen_name IS NOT NULL
    AND t2.screen_name IS NOT NULL
    AND h.text IS NOT NULL
    ORDER BY common_hashtag, retweet_source
    LIMIT 20;
    """
]
