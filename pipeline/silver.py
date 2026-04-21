# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # Silver Layer — Cleaned comments

# COMMAND ----------

from pyspark import pipelines as dp
import pyspark.sql.functions as F

CATALOG       = "anirvansen_catalog"
BRONZE_SCHEMA = "yt_bronze"
SILVER_SCHEMA = "yt_silver"

# COMMAND ----------

@dp.expect("valid_comment_id", "comment_id IS NOT NULL")
@dp.expect("has_text", "LENGTH(text) > 0")
@dp.table(
    name=f"{CATALOG}.{SILVER_SCHEMA}.comments",
    cluster_by=["channel_handle"],
    comment="Cleaned YouTube comments with typed columns"
)
def silver_comments():
    return (
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.raw_comments")
        .select(
            F.col("comment_id"),
            F.col("video_id"),
            F.col("video_title").alias("title"),
            F.col("video_url"),
            F.col("channel_handle"),
            F.col("author_name"),
            F.trim(F.col("text")).alias("text"),
            F.col("like_count"),
            F.col("reply_count"),
            F.to_timestamp("published_at").alias("published_at"),
            F.col("is_reply"),
            F.col("parent_id"),
            F.col("_ingested_at").alias("processed_at"),
        )
        .filter(F.length(F.trim(F.col("text"))) > 0)
    )
