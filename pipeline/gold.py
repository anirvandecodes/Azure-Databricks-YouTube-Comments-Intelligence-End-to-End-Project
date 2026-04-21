# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # Gold Layer — Comment sentiment metrics (Materialized View)

# COMMAND ----------

from pyspark import pipelines as dp

CATALOG       = "anirvansen_catalog"
SILVER_SCHEMA = "yt_silver"
GOLD_SCHEMA   = "yt_gold"

# COMMAND ----------

@dp.materialized_view(
    name=f"{CATALOG}.{GOLD_SCHEMA}.comment_metrics",
    cluster_by=["channel_handle"],
    comment="Per-video comment analytics with sentiment breakdown using ai_analyze_sentiment()"
)
def gold_comment_metrics():
    return spark.sql(f"""
        WITH scored AS (
            SELECT
                video_id,
                channel_handle,
                title,
                video_url,
                like_count,
                reply_count,
                published_at,
                ai_analyze_sentiment(text) AS sentiment
            FROM {CATALOG}.{SILVER_SCHEMA}.comments
            WHERE NOT is_reply
        )
        SELECT
            video_id,
            channel_handle,
            title,
            video_url,
            COUNT(*)                                                              AS total_comments,
            SUM(like_count)                                                       AS total_likes,
            ROUND(AVG(like_count), 2)                                             AS avg_likes_per_comment,
            SUM(reply_count)                                                      AS total_replies,
            SUM(CASE WHEN sentiment = 'positive' THEN 1 ELSE 0 END)               AS positive_comments,
            SUM(CASE WHEN sentiment = 'negative' THEN 1 ELSE 0 END)               AS negative_comments,
            SUM(CASE WHEN sentiment = 'neutral'  THEN 1 ELSE 0 END)               AS neutral_comments,
            ROUND(
                SUM(CASE WHEN sentiment = 'positive' THEN  1.0
                         WHEN sentiment = 'negative' THEN -1.0
                         ELSE 0.0 END)
                / NULLIF(COUNT(*), 0), 3
            )                                                                     AS sentiment_score,
            MAX(published_at)                                                     AS latest_comment_at,
            CURRENT_TIMESTAMP()                                                   AS computed_at
        FROM scored
        GROUP BY video_id, channel_handle, title, video_url
    """)
