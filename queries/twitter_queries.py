RAW_TWITTER_QUERIES = [
# Query 1
"""
SELECT COUNT(*) AS english_tweet_count
FROM tweets
WHERE data->>'lang' = 'en';
""",
# Query 2
"""
SELECT data->>'source' AS source, COUNT(*) AS tweet_count
FROM tweets
GROUP BY data->>'source'
ORDER BY tweet_count DESC;
""",
# Query 3
"""
SELECT 
    data->'retweeted_status'->'user'->>'screen_name' AS user, 
    SUM(CAST(data->'retweeted_status'->>'retweet_count' AS INTEGER)) AS total_retweets
FROM tweets
WHERE (data->'retweeted_status'->>'id_str') IS NOT NULL
GROUP BY data->'retweeted_status'->'user'->>'screen_name'
ORDER BY total_retweets DESC
LIMIT 10;
""",
# Query 4
"""
SELECT 
    (data->>'in_reply_to_user_id_str') AS user_id, 
    COUNT(*) AS reply_count
FROM tweets
WHERE 
    (data->>'in_reply_to_user_id_str') NOT LIKE 'null'
    AND (data->>'in_reply_to_user_id_str') IS NOT NULL
GROUP BY user_id
ORDER BY reply_count DESC
LIMIT 15;
""",
# Query 5
# """
# SELECT 
#     data->'user'->>'screen_name' AS screen_name, 
#     ANY_VALUE(data->'user'->>'followers_count') AS followers_count,
#     COUNT(*) AS tweet_count
# FROM tweets
# WHERE CAST(data->'user'->>'followers_count' AS INTEGER) > 1000
# GROUP BY screen_name 
# ORDER BY tweet_count DESC
# LIMIT 10;
# """,
# Query 6
# """
# WITH Hashtags AS (
#     SELECT UNNEST(ARRAY_AGG(lower(data->'entities'->'hashtags'->>'text'))) AS hashtag
#     FROM tweets
# ), TopHashtags AS (
#     SELECT hashtag, COUNT(*) AS count
#     FROM Hashtags
#     GROUP BY hashtag
#     ORDER BY count DESC
#     LIMIT 10
# )
#  SELECT 
#     th.hashtag AS main_hashtag, 
#     ARRAY_AGG(ch.hashtag) AS co_occurring_hashtags
#  FROM TopHashtags th
#  JOIN Hashtags ch ON th.hashtag <> ch.hashtag
#  GROUP BY th.hashtag;
# """,
# Query 7
# """
# SELECT 
#     LOWER(data->'entities'->'hashtags'->>'text') AS hashtag, 
#     COUNT(*) AS count
# FROM tweets
# WHERE LOWER(data->>'text') LIKE '%lady gaga%'
# GROUP BY LOWER(data->'entities'->'hashtags'->>'text')
# ORDER BY count DESC
# LIMIT 10;
# """,
#  Query 8
"""
SELECT ROUND(
    100.0 * COUNT(DISTINCT data->'user'->>'id_str') / 
    (SELECT COUNT(DISTINCT data->'user'->>'id_str') FROM tweets), 2) AS percentage
FROM tweets
WHERE LOWER(data->>'text') LIKE '%covid-19%';
""",
# Query 9
"""
SELECT 
    (initial_tweet.data->>'id_str') AS initial_tweet_id,
    (initial_tweet.data->'user'->>'screen_name') AS initial_author,
    (retweet1.data->'user'->>'screen_name') AS first_retweeter,
    (retweet2.data->'user'->>'screen_name') AS second_retweeter
FROM 
    tweets initial_tweet, 
    tweets retweet1, 
    tweets retweet2
WHERE (retweet1.data->'retweeted_status'->>'id_str') = (initial_tweet.data->>'id_str')
  AND (retweet2.data->'retweeted_status'->>'id_str') = (retweet1.data->>'id_str')
ORDER BY initial_tweet_id
LIMIT 20;
""",
# Query 10
# """
# SELECT 
#     ANY_VALUE(data->'retweeted_status'->'user'->>'screen_name') AS original_author,
#     ANY_VALUE(data->'user'->>'screen_name') AS retweeter,
#     (
#         SELECT unnest.hashtag 
#         FROM UNNEST(array_agg(data->'entities'->'hashtags'->>'text')) AS unnest(hashtag)
#         GROUP BY unnest.hashtag 
#         ORDER BY COUNT(*) DESC 
#         LIMIT 1
#     ) AS common_hashtag,
#     ANY_VALUE(data->>'source') AS source_platform
# FROM tweets
# WHERE (data->'retweeted_status'->>'id_str') IS NOT NULL;
# """,
# Query 11
"""
SELECT 
    (original_tweet.data->>'id_str') AS original_tweet_id,
    (original_tweet.data->'user'->>'screen_name') AS original_author,
    (retweet.data->'user'->>'screen_name') AS retweeter,
    CAST(retweet.data->'retweeted_status'->>'retweet_count' AS INTEGER) AS retweet_retweet_count
FROM tweets AS original_tweet, tweets AS retweet
WHERE (retweet.data->'retweeted_status'->>'id_str') = (original_tweet.data->>'id_str')
GROUP BY 
    original_tweet_id,
    original_author,
    retweeter,
    retweet_retweet_count
ORDER BY retweet_retweet_count DESC
LIMIT 10;
""",
# Query 12
"""
SELECT 
    ANY_VALUE(user_info.data->'user'->>'screen_name') AS screen_name,
    SUM(CAST(user_info.data->'user'->>'followers_count' AS INT)) AS followers_count,
    COUNT(DISTINCT reply.id_str) AS reply_count,
    COUNT(DISTINCT retweet.id_str) AS retweet_count
FROM 
    tweets user_info,
    (
        SELECT 
            tweets.data->>'id_str' AS id_str,
            (tweets.data->'retweeted_status'->'user'->>'id_str') AS retweeter
        FROM tweets
    ) AS retweet,
    (
        SELECT 
            tweets.data->>'id_str' AS id_str,
            (tweets.data->>'in_reply_to_user_id_str') AS reply_to_user
        FROM tweets
    ) AS reply
WHERE 
    CAST(user_info.data->'user'->>'followers_count' AS INTEGER) > 1000
    AND reply.reply_to_user = (user_info.data->'user'->>'id_str')
    AND retweet.retweeter = (user_info.data->'user'->>'id_str')
GROUP BY 
    (user_info.data->'user'->>'id_str') 
ORDER BY 
    followers_count DESC, 
    (reply_count + retweet_count) DESC
LIMIT 15;
"""
]

MATERIALIZED_TWITTER_QUERIES = [
# Query 1
"""
SELECT COUNT(*) AS english_tweet_count
FROM tweets
WHERE lang = 'en';
""",
# Query 2
"""
SELECT source, COUNT(*) AS tweet_count
FROM tweets
GROUP BY source
ORDER BY tweet_count DESC;
""",
# Query 3 
"""
SELECT 
    retweetedStatus_user_screenName AS user, 
    SUM(retweetedStatus_retweetCount) AS total_retweets
FROM tweets
WHERE retweetedStatus_idStr IS NOT NULL
GROUP BY retweetedStatus_user_screenName
ORDER BY total_retweets DESC
LIMIT 10;
""",
# Query 4
"""
SELECT inReplyToUserIdStr AS user_id, COUNT(*) AS reply_count
FROM tweets
WHERE 
    inReplyToUserIdStr IS NOT NULL
    AND inReplyToUserIdStr not like 'null'
GROUP BY inReplyToUserIdStr
ORDER BY reply_count DESC
LIMIT 15;
""",
# Query 5
# """
# SELECT 
#     user_screenName AS screen_name,
#     ANY_VALUE(user_followersCount) AS followers_count,
#     COUNT(*) AS tweet_count
# FROM tweets
# WHERE CAST(user_followersCount AS INTEGER) > 1000
# GROUP BY screen_name 
# ORDER BY tweet_count DESC
# LIMIT 10;
# """,
# Query 6
# """
# WITH Hashtags AS (
#     SELECT UNNEST(ARRAY_AGG(lower(entities_hashtags_text))) AS hashtag
#     FROM tweets
# ), TopHashtags AS (
#     SELECT hashtag, COUNT(*) AS count
#     FROM Hashtags
#     GROUP BY hashtag
#     ORDER BY count DESC
#     LIMIT 10
# )
# SELECT 
# th.hashtag AS main_hashtag, 
# ARRAY_AGG(ch.hashtag) AS co_occurring_hashtags
# FROM TopHashtags th
# JOIN Hashtags ch ON th.hashtag <> ch.hashtag
# GROUP BY th.hashtag;
# """,
# Query 7
# """
# SELECT 
#     LOWER(entities_hashtags_text) AS hashtag, 
#     COUNT(*) AS count
# FROM tweets
# WHERE LOWER(text) LIKE '%lady gaga%'
# GROUP BY LOWER(entities_hashtags_text)
# ORDER BY count DESC
# LIMIT 10;
# """,
# Query 8
"""
SELECT ROUND(
    100.0 * COUNT(DISTINCT user_idStr) / 
    (SELECT COUNT(DISTINCT user_idStr) FROM tweets), 2) AS percentage
FROM tweets
WHERE lower(text) LIKE '%covid-19%';
""",
# Query 9
"""
SELECT 
    initial_tweet.idStr AS initial_tweet_id,
    initial_tweet.retweetedStatus_user_screenName AS initial_author,
    retweet1.retweetedStatus_user_screenName AS first_retweeter,
    retweet2.retweetedStatus_user_screenName AS second_retweeter
FROM tweets AS initial_tweet
JOIN tweets AS retweet1 
    ON retweet1.retweetedStatus_idStr = initial_tweet.idStr
JOIN tweets AS retweet2 
    ON retweet2.retweetedStatus_idStr = retweet1.idStr
ORDER BY initial_tweet_id
LIMIT 20;
""",
# Query 10
# """
# SELECT 
#     ANY_VALUE(retweetedStatus_idStr) AS original_tweet_id,
#     ANY_VALUE(user_screenName) AS retweeter,
#     (
#         SELECT unnest.hashtag 
#         FROM UNNEST(string_to_array(ANY_VALUE(entities_hashtags_text), ',')) AS unnest(hashtag)
#         GROUP BY unnest.hashtag 
#         ORDER BY COUNT(*) DESC 
#         LIMIT 1
#     ) AS common_hashtag,
#     ANY_VALUE(source) AS source_platform
# FROM tweets
# WHERE retweetedStatus_idStr IS NOT NULL;
# """,
# Query 11
"""
SELECT 
    original_tweet.idStr AS original_tweet_id,
    original_tweet.user_screenName AS original_author,
    retweet.user_screenName AS retweeter,
    retweet.retweetedStatus_retweetCount AS retweet_retweet_count
FROM tweets AS original_tweet, tweets AS retweet
WHERE retweet.retweetedStatus_idStr = original_tweet.idStr
GROUP BY 
    original_tweet_id,
    original_author,
    retweeter,
    retweet_retweet_count
ORDER BY retweet_retweet_count DESC
LIMIT 10;
""",
# Query 12
"""
SELECT 
    ANY_VALUE(user_info.user_screenName) AS screen_name,
    SUM(user_info.user_followersCount) AS followers_count,
    COUNT(DISTINCT reply.id_str) AS reply_count,
    COUNT(DISTINCT retweet.id_str) AS retweet_count
FROM 
    tweets user_info,
    (
        SELECT 
            tweets.idStr AS id_str,
            tweets.retweetedStatus_user_idStr AS retweeter
        FROM tweets 
    ) AS retweet,
    (
        SELECT 
            tweets.idStr AS id_str,
            tweets.inReplyToUserIdStr AS reply_to_user
        FROM tweets
    ) AS reply
WHERE 
    user_info.user_followersCount > 1000
    AND reply.reply_to_user = user_info.user_idStr
    AND retweet.retweeter = user_info.user_idStr
GROUP BY user_info.user_idStr
ORDER BY 
    followers_count DESC, 
    (reply_count + retweet_count) DESC
LIMIT 15;

"""
]
