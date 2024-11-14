RAW_TWITTER_QUERIES = [
     """
     SELECT COUNT(*) AS english_tweet_count
     FROM tweets
     WHERE data->>'lang' = 'en';
         """,
         """
     SELECT data->>'source' AS source, COUNT(*) AS tweet_count
     FROM tweets
     GROUP BY data->>'source'
     ORDER BY tweet_count DESC;
     """,
     """
     SELECT 
         data->'retweeted_status'->'user'->>'screen_name' AS user, 
         SUM(CAST(data->'retweeted_status'->>'retweet_count' AS INTEGER)) AS total_retweets
     FROM tweets
     GROUP BY data->'retweeted_status'->'user'->>'screen_name'
     ORDER BY total_retweets DESC
     LIMIT 10;

     """,
     """
 SELECT CAST(data->>'in_reply_to_user_id' AS TEXT) AS user_id, COUNT(*) AS reply_count
 FROM tweets
 WHERE CAST(data->>'in_reply_to_user_id' AS TEXT) IS NOT NULL
 GROUP BY user_id
 ORDER BY reply_count DESC
 LIMIT 15;

     """,
     """
 SELECT data->'user'->>'screen_name' AS screen_name, 
        COUNT(*) AS tweet_count
 FROM tweets
 WHERE CAST(data->'user'->>'followers_count' AS INTEGER) > 1000
 GROUP BY data->'user'->>'screen_name'
 ORDER BY tweet_count DESC
 LIMIT 10;

     """,
     """
 WITH Hashtags AS (
     SELECT unnest(array_agg(lower(data->'entities'->'hashtags'->>'text'))) AS hashtag
     FROM tweets
 ), TopHashtags AS (
     SELECT hashtag, COUNT(*) AS count
     FROM Hashtags
     GROUP BY hashtag
     ORDER BY count DESC
     LIMIT 10
 )
 SELECT th.hashtag AS main_hashtag, 
        array_agg(ch.hashtag) AS co_occurring_hashtags
 FROM TopHashtags th
 JOIN Hashtags ch ON th.hashtag <> ch.hashtag
 GROUP BY th.hashtag;
     """,
     """
 SELECT LOWER(data->'entities'->'hashtags'->>'text') AS hashtag, COUNT(*) AS count
 FROM tweets
 WHERE LOWER(data->>'text') LIKE '%lady gaga%'
 GROUP BY LOWER(data->'entities'->'hashtags'->>'text')
 ORDER BY count DESC
 LIMIT 10;

     """,
     """
 SELECT ROUND(
        100.0 * COUNT(DISTINCT data->'user'->>'id') / 
        (SELECT COUNT(DISTINCT data->'user'->>'id') FROM tweets), 2) AS percentage
 FROM tweets
 WHERE LOWER(data->>'text') LIKE '%covid-19%';

     """,
     """SELECT 
     initial_tweet.data->>'id_str' AS initial_tweet_id,
     initial_tweet.data->'user'->>'screen_name' AS initial_author,
     retweet1.data->'user'->>'screen_name' AS first_retweeter,
     retweet2.data->'user'->>'screen_name' AS second_retweeter
 FROM tweets AS initial_tweet
 JOIN tweets AS retweet1 
     ON retweet1.data->'retweeted_status'->>'id_str' = initial_tweet.data->>'id_str'
 JOIN tweets AS retweet2 
     ON retweet2.data->'retweeted_status'->>'id_str' = retweet1.data->>'id_str'
 ORDER BY initial_tweet_id
 LIMIT 20;
 """,
     """
 SELECT 
     ANY_VALUE(data->'retweeted_status'->'user'->>'screen_name') AS original_author,
     ANY_VALUE(data->'user'->>'screen_name') AS retweeter,
     (SELECT unnest.hashtag 
      FROM UNNEST(array_agg(data->'entities'->'hashtags'->>'text')) AS unnest(hashtag)
      GROUP BY unnest.hashtag 
      ORDER BY COUNT(*) DESC 
      LIMIT 1) AS common_hashtag,
     ANY_VALUE(data->>'source') AS source_platform
 FROM tweets;

     """,
     """
     SELECT 
     original_tweet.data->>'id_str' AS original_tweet_id,
     original_tweet.data->'user'->>'screen_name' AS original_author,
     retweet.data->'user'->>'screen_name' AS retweeter,
     CAST(original_tweet.data->>'retweet_count' AS INTEGER) AS original_retweet_count,
     CAST(retweet.data->>'retweet_count' AS INTEGER) AS retweet_retweet_count
 FROM tweets AS retweet
 JOIN tweets AS original_tweet 
     ON retweet.data->'retweeted_status'->>'id_str' = original_tweet.data->>'id_str'
 ORDER BY original_retweet_count DESC
 LIMIT 10;
 """,
 """
 SELECT 
     user_info.data->>'screen_name' AS screen_name,
     CAST(user_info.data->>'followers_count' AS INTEGER) AS followers_count,
     COUNT(DISTINCT retweet.data->>'id_str') AS retweet_count,
     COUNT(DISTINCT reply.data->>'id_str') AS reply_count
 FROM tweets AS user_info
 LEFT JOIN tweets AS retweet 
     ON retweet.data->'retweeted_status'->'user'->>'id_str' = user_info.data->>'id_str'
 LEFT JOIN tweets AS reply 
     ON reply.data->>'in_reply_to_user_id' = user_info.data->>'id_str'
 WHERE CAST(user_info.data->>'followers_count' AS INTEGER) > 1000
 GROUP BY user_info.data->>'screen_name', user_info.data->>'followers_count'
 ORDER BY followers_count DESC, (retweet_count + reply_count) DESC
 LIMIT 15;
 """
]

MATERIALIZED_TWITTER_QUERIES = [
     """
 SELECT COUNT(*) AS english_tweet_count
 FROM tweets
 WHERE lang = 'en';

     """,
     """
 SELECT source, COUNT(*) AS tweet_count
 FROM tweets
 GROUP BY source
 ORDER BY tweet_count DESC;

     """,
     """
 SELECT 
     user_screen_name AS user, 
     COUNT(*) AS total_retweets
 FROM tweets
 WHERE retweeted_status_id_str IS NOT NULL
 GROUP BY user_screen_name
 ORDER BY total_retweets DESC
 LIMIT 10;

     """,
     """
 SELECT in_reply_to_user_id_str AS user_id, COUNT(*) AS reply_count
 FROM tweets
 WHERE in_reply_to_user_id_str IS NOT NULL
 GROUP BY in_reply_to_user_id_str
 ORDER BY reply_count DESC
 LIMIT 15;

     """,
     """
 SELECT user_screen_name, COUNT(*) AS tweet_count
 FROM tweets
 WHERE CAST(user_followers_count AS INTEGER) > 1000
 GROUP BY user_screen_name
 ORDER BY tweet_count DESC
 LIMIT 10;

     """,
     """
 WITH Hashtags AS (
     SELECT unnest(array_agg(lower(hashtag_text))) AS hashtag
     FROM tweets
 ), TopHashtags AS (
     SELECT hashtag, COUNT(*) AS count
     FROM Hashtags
     GROUP BY hashtag
     ORDER BY count DESC
     LIMIT 10
 )
 SELECT th.hashtag AS main_hashtag, 
        array_agg(ch.hashtag) AS co_occurring_hashtags
 FROM TopHashtags th
 JOIN Hashtags ch ON th.hashtag <> ch.hashtag
 GROUP BY th.hashtag;

     """,
     """
 SELECT LOWER(hashtag_text) AS hashtag, COUNT(*) AS count
 FROM tweets
 WHERE LOWER(text) LIKE '%lady gaga%'
 GROUP BY LOWER(hashtag_text)
 ORDER BY count DESC
 LIMIT 10;

     """,
     """
 SELECT ROUND(
        100.0 * COUNT(DISTINCT user_id_str) / 
        (SELECT COUNT(DISTINCT user_id_str) FROM tweets), 2) AS percentage
 FROM tweets
 WHERE lower(text) LIKE '%covid-19%';

     """,
     """
 SELECT 
     initial_tweet.id_str AS initial_tweet_id,
     initial_tweet.user_screen_name AS initial_author,
     retweet1.user_screen_name AS first_retweeter,
     retweet2.user_screen_name AS second_retweeter
 FROM tweets AS initial_tweet
 JOIN tweets AS retweet1 
     ON retweet1.retweeted_status_id_str = initial_tweet.id_str
 JOIN tweets AS retweet2 
     ON retweet2.retweeted_status_id_str = retweet1.id_str
 ORDER BY initial_tweet_id
 LIMIT 20;

    """,
    """
SELECT 
    ANY_VALUE(retweeted_status_id_str) AS original_tweet_id,
    ANY_VALUE(user_screen_name) AS retweeter,
    (SELECT unnest.hashtag 
     FROM UNNEST(string_to_array(ANY_VALUE(hashtag_text), ',')) AS unnest(hashtag)
     GROUP BY unnest.hashtag 
     ORDER BY COUNT(*) DESC 
     LIMIT 1) AS common_hashtag,
    ANY_VALUE(source) AS source_platform
FROM tweets
WHERE retweeted_status_id_str IS NOT NULL;

""",
 """
 SELECT 
     original_tweet.id_str AS original_tweet_id,
     original_tweet.user_screen_name AS original_author,
     retweet.user_screen_name AS retweeter,
     COUNT(*) AS retweet_count
 FROM tweets AS retweet
 JOIN tweets AS original_tweet 
     ON retweet.retweeted_status_id_str = original_tweet.id_str
 GROUP BY original_tweet.id_str, original_tweet.user_screen_name, retweet.user_screen_name
 ORDER BY retweet_count DESC
 LIMIT 10;

 """,
 """
 SELECT 
     user_screen_name,
     CAST(user_followers_count AS INTEGER) AS followers_count,
     COUNT(CASE WHEN retweeted_status_id_str IS NOT NULL THEN 1 ELSE NULL END) AS retweet_count,
     COUNT(CASE WHEN in_reply_to_user_id_str IS NOT NULL THEN 1 ELSE NULL END) AS reply_count
 FROM tweets
 WHERE CAST(user_followers_count AS INTEGER) > 1000
 GROUP BY user_screen_name, user_followers_count
 ORDER BY followers_count DESC, (retweet_count + reply_count) DESC
 LIMIT 15;

 """
]
